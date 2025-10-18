// server.js
import express from "express";
import http from "http";
import { Server } from "socket.io";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import {
  S3Client,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import multer from "multer";
import { Upload } from "@aws-sdk/lib-storage";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const server = http.createServer(app);

// === SOCKET.IO SETUP ===
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  maxHttpBufferSize: 10 * 1024 * 1024, // 10MB per message
  pingTimeout: 60000,
  pingInterval: 25000,
});

app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ extended: true, limit: "50mb" }));
server.timeout = 0; // no request timeout

// === S3 CLIENT ===
const s3Client = new S3Client({
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
  region: process.env.AWS_REGION,
});

const S3_BUCKET = process.env.AWS_BUCKET_NAME;
if (!S3_BUCKET) {
  console.error("❌ AWS_BUCKET_NAME not set in .env");
  process.exit(1);
}

// Active multipart uploads
const activeMultipartUploads = new Map(); // { uploadId: { Key, roomId, timeoutHandle } }
const roomMessages = {}; // last 5 messages per room

// === Helper: auto-abort multipart uploads ===
function scheduleMultipartAbort(uploadId, key, ttlMs = 4 * 60 * 1000, roomId = null) {
  if (activeMultipartUploads.has(uploadId)) {
    clearTimeout(activeMultipartUploads.get(uploadId).timeoutHandle);
  }
  const timeoutHandle = setTimeout(async () => {
    try {
      await s3Client.send(
        new AbortMultipartUploadCommand({ Bucket: S3_BUCKET, Key: key, UploadId: uploadId })
      );
      activeMultipartUploads.delete(uploadId);
      if (roomId) io.to(roomId).emit("file-removed", { s3Key: key });
      console.log(`🛑 Aborted multipart ${uploadId} and cleaned up ${key}`);
    } catch (err) {
      console.error("Error aborting multipart:", err);
    }
  }, ttlMs);

  activeMultipartUploads.set(uploadId, { Key: key, timeoutHandle, roomId });
}

// === SOCKET.IO HANDLERS ===
io.on("connection", (socket) => {
  console.log("✅ Client connected:", socket.id);

  // Join room
  socket.on("join-room", (roomId) => {
    socket.join(roomId);
    if (!roomMessages[roomId]) roomMessages[roomId] = { messages: [], contentType: "text" };

    // Send last messages + current content type
    socket.emit("room-messages", roomMessages[roomId].messages);
    socket.emit("room-contentType", roomMessages[roomId].contentType);
  });

  // Handle content type changes
  socket.on("room-contentType", ({ roomId, type }) => {
    if (!roomMessages[roomId]) roomMessages[roomId] = { messages: [], contentType: "text" };
    roomMessages[roomId].contentType = type;

    // Broadcast to all users in the room
    io.to(roomId).emit("room-contentType", type);
  });

  // Handle text messages
  socket.on("room-message", ({ roomId, type, content, fileName, fileType, data, uploadedAt }) => {
    if (!roomMessages[roomId]) roomMessages[roomId] = { messages: [], contentType: "text" };
    const message = { type, content, fileName, fileType, data, uploadedAt };

    roomMessages[roomId].messages.push(message);
    if (roomMessages[roomId].messages.length > 5) roomMessages[roomId].messages.shift();

    io.to(roomId).emit("room-message", message);
  });

  // Step 1: Initiate multipart upload
  // Step 1: Initiate multipart upload
  socket.on("initiate-multipart", async ({ roomId, fileName, fileType }) => {
    try {
      const Key = `${Date.now()}_${fileName}`;

      const createCmd = new CreateMultipartUploadCommand({
        Bucket: S3_BUCKET,
        Key,
        ContentType: fileType,
      });

      const { UploadId } = await s3Client.send(createCmd);

      // ✅ Store on socket session
      if (!socket.uploadSessions) {
        socket.uploadSessions = {};
      }
      socket.uploadSessions[UploadId] = { Key };

      // ✅ Also track globally so complete-multipart can see it
      activeMultipartUploads.set(UploadId, {
        Key,
        roomId,
        timeoutHandle: null,
      });

      // auto-abort if no activity after 4 mins
      scheduleMultipartAbort(UploadId, Key, 4 * 60 * 1000, roomId);

      socket.emit("multipart-initiated", { uploadId: UploadId, key: Key });
      console.log(`✅ Multipart upload started: ${Key}, UploadId: ${UploadId}`);
    } catch (err) {
      console.error("initiate-multipart error:", err);
      socket.emit("initiate-error", { message: err.message });
    }
  });


  // Step 2: Get presigned URLs for part numbers
  // Step 2: Provide presigned URLs for given parts
  socket.on("get-presigned-urls", async ({ uploadId, partNumbers }) => {
    try {
      if (!socket.uploadSessions || !socket.uploadSessions[uploadId]) {
        socket.emit("presign-error", { message: "UploadId not found or expired" });
        return;
      }

      const { Key } = socket.uploadSessions[uploadId];
      const urls = [];

      for (const partNumber of partNumbers) {
        const cmd = new UploadPartCommand({
          Bucket: S3_BUCKET,
          Key,
          UploadId: uploadId,
          PartNumber: partNumber,
        });

        const url = await getSignedUrl(s3Client, cmd, { expiresIn: 3600 });
        urls.push({ partNumber, url });
      }

      socket.emit("presigned-urls", { uploadId, urls });
      console.log(`✅ Sent presigned URLs for ${Key} (parts: ${partNumbers.length})`);
    } catch (err) {
      console.error("get-presigned-urls error:", err);
      socket.emit("presign-error", { message: err.message });
    }
  });


  // Step 3: Complete multipart upload
  socket.on("complete-multipart", async ({ uploadId, parts }) => {
    try {
      if (!activeMultipartUploads.has(uploadId)) {
        socket.emit("complete-error", { message: "UploadId not found or expired" });
        return;
      }
      const { Key, roomId } = activeMultipartUploads.get(uploadId);

      const res = await s3Client.send(
        new CompleteMultipartUploadCommand({
          Bucket: S3_BUCKET,
          Key,
          UploadId: uploadId,
          MultipartUpload: { Parts: parts },
        })
      );

      clearTimeout(activeMultipartUploads.get(uploadId).timeoutHandle);
      activeMultipartUploads.delete(uploadId);

      const location = res.Location || `s3://${S3_BUCKET}/${Key}`;
      const fileMessage = {
        type: "file",
        fileName: path.basename(Key),
        data: location,
        s3Key: Key,
      };

      if (!roomMessages[roomId]) roomMessages[roomId] = { messages: [], contentType: "text" };
      roomMessages[roomId].messages.push(fileMessage);
      if (roomMessages[roomId].messages.length > 5) roomMessages[roomId].messages.shift();

      io.to(roomId).emit("room-message", fileMessage);
      socket.emit("complete-success", { uploadId, location });

      // Delete after 4 min
      setTimeout(async () => {
        try {
          await s3Client.send(new DeleteObjectCommand({ Bucket: S3_BUCKET, Key }));
          io.to(roomId).emit("file-removed", { s3Key: Key });
          console.log(`🗑️ Deleted from S3: ${Key}`);
        } catch (err) {
          console.error("Error deleting file:", err);
        }
      }, 4 * 60 * 1000);

      console.log(`✅ Completed upload: ${location}`);
    } catch (err) {
      console.error("complete-multipart error:", err);
      socket.emit("complete-error", { message: err.message });
    }
  });

  socket.on("disconnect", () => {
    console.log("❌ Client disconnected:", socket.id);
  });
});

// === REST FALLBACK ENDPOINTS (optional) ===

// Presign many parts
app.post("/presign-multipart", async (req, res) => {
  try {
    const { fileName, fileType, partsCount = 1 } = req.body;
    if (!fileName) return res.status(400).json({ message: "fileName required" });

    const key = `uploads/${Date.now()}-${fileName}`;
    const createRes = await s3Client.send(
      new CreateMultipartUploadCommand({ Bucket: S3_BUCKET, Key: key, ContentType: fileType })
    );

    const uploadId = createRes.UploadId;
    scheduleMultipartAbort(uploadId, key);

    const urls = [];
    for (let i = 1; i <= partsCount; i++) {
      const cmd = new UploadPartCommand({ Bucket: S3_BUCKET, Key: key, UploadId: uploadId, PartNumber: i });
      const url = await getSignedUrl(s3Client, cmd, { expiresIn: 3600 });
      urls.push({ partNumber: i, url });
    }

    res.json({ uploadId, key, urls });
  } catch (err) {
    console.error("presign-multipart error:", err);
    res.status(500).json({ message: err.message });
  }
});

// Complete multipart
app.post("/complete-multipart", async (req, res) => {
  try {
    const { uploadId, key, parts } = req.body;
    if (!uploadId || !key || !parts) return res.status(400).json({ message: "uploadId, key and parts required" });

    const result = await s3Client.send(
      new CompleteMultipartUploadCommand({
        Bucket: S3_BUCKET,
        Key: key,
        UploadId: uploadId,
        MultipartUpload: { Parts: parts },
      })
    );

    if (activeMultipartUploads.has(uploadId)) {
      clearTimeout(activeMultipartUploads.get(uploadId).timeoutHandle);
      activeMultipartUploads.delete(uploadId);
    }

    res.json({ message: "Completed", location: result.Location || `s3://${S3_BUCKET}/${key}` });
  } catch (err) {
    console.error("complete-multipart REST error:", err);
    res.status(500).json({ message: err.message });
  }
});

// Abort multipart
app.post("/abort-multipart", async (req, res) => {
  try {
    const { uploadId, key } = req.body;
    if (!uploadId || !key) return res.status(400).json({ message: "uploadId and key required" });

    await s3Client.send(new AbortMultipartUploadCommand({ Bucket: S3_BUCKET, Key: key, UploadId: uploadId }));
    if (activeMultipartUploads.has(uploadId)) {
      clearTimeout(activeMultipartUploads.get(uploadId).timeoutHandle);
      activeMultipartUploads.delete(uploadId);
    }
    res.json({ message: "Aborted" });
  } catch (err) {
    console.error("abort-multipart error:", err);
    res.status(500).json({ message: err.message });
  }
});

// Single small file upload (fallback)
const upload = multer({ storage: multer.memoryStorage() });
app.post("/upload", upload.single("file"), async (req, res) => {
  try {
    const file = req.file;
    const key = `uploads/${Date.now()}-${file.originalname}`;

    const uploader = new Upload({
      client: s3Client,
      params: { Bucket: S3_BUCKET, Key: key, Body: file.buffer, ContentType: file.mimetype },
    });

    const result = await uploader.done();
    res.json({ fileName: file.originalname, s3Url: result.Location || `s3://${S3_BUCKET}/${key}` });
  } catch (err) {
    console.error("upload error:", err);
    res.status(500).json({ message: err.message });
  }
});

// === START SERVER ===
const PORT = process.env.PORT || 5000;
server.listen(PORT, () => console.log(`🚀 Server running at http://localhost:${PORT}`));
