import express from "express";
import { createServer } from "http";
import { Server } from "socket.io";
import { createServer as createViteServer } from "vite";
import path from "path";
import * as admin from "firebase-admin";
import firebaseConfig from "./firebase-applet-config.json";

// --- KAFKA EMULATOR ---
type Message = { value: string };
type EachMessageHandler = (payload: { topic: string; partition: number; message: Message }) => Promise<void>;

class KafkaEmulator {
  private topics: Map<string, Set<EachMessageHandler>> = new Map();

  constructor() {
    console.log("Kafka Emulator Initialized");
  }

  producer() {
    return {
      send: async ({ topic, messages }: { topic: string; messages: Message[] }) => {
        const handlers = this.topics.get(topic);
        if (!handlers) return;
        
        for (const message of messages) {
          // In Kafka, events are asynchronous
          handlers.forEach(handler => {
            handler({ topic, partition: 0, message }).catch(err => 
              console.error(`Consumer error in topic ${topic}:`, err)
            );
          });
        }
      }
    };
  }

  consumer({ groupId }: { groupId: string }) {
    console.log(`Consumer Group [${groupId}] created`);
    return {
      subscribe: async ({ topic }: { topic: string }) => {
        if (!this.topics.has(topic)) {
          this.topics.set(topic, new Set());
        }
      },
      run: async ({ eachMessage }: { eachMessage: EachMessageHandler }) => {
        // Find all topics this consumer might be interested in based on previous subscribe calls
        // For simulation, we'll just register this handler to the topics it subscribed to.
        this.topics.forEach((handlers, topic) => {
          handlers.add(eachMessage);
        });
      }
    };
  }
}

const kafka = new KafkaEmulator();
const producer = kafka.producer();

// --- FIREBASE ADMIN SETUP ---
try {
  admin.initializeApp({
    projectId: firebaseConfig.projectId,
  });
} catch (e) {
  console.warn("Firebase Admin failed to initialize. Falling back to mock verification.");
}

async function startServer() {
  const app = express();
  const httpServer = createServer(app);
  const io = new Server(httpServer, {
    cors: {
      origin: "*",
      methods: ["GET", "POST"]
    }
  });

  const PORT = 3000;

  // --- KAFKA CONSUMERS ---

  // Consumer Group A: Real-time broadcast to connected sockets
  const socketConsumer = kafka.consumer({ groupId: "socket-group" });
  await socketConsumer.subscribe({ topic: "location-updates" });
  await socketConsumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value);
      io.emit("location-updated", data);
    }
  });

  // Consumer Group B: Persistence / Database Logging
  // This lives in the same process but follows Kafka patterns
  const dbConsumer = kafka.consumer({ groupId: "db-group" });
  await dbConsumer.subscribe({ topic: "location-updates" });
  await dbConsumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value);
      // In a real app, this would write to Firestore
      console.log(`[DB Consumer] Persistent recording for user ${data.userId}: ${data.lat}, ${data.lng}`);
      // NOTE: We don't write to Firestore from the server here to avoid needing service account keys 
      // in environments that might not have them. The client also writes its own location in this demo 
      // as per Firestore security rules.
    }
  });

  // --- SOCKET.IO LOGIC ---
  io.use(async (socket, next) => {
    const token = socket.handshake.auth.token;
    if (!token) {
      return next(new Error("Authentication error"));
    }
    
    try {
      // In a real production app, verify with admin.auth().verifyIdToken(token)
      // For this demo, we'll simulate verification
      const uid = socket.handshake.auth.uid; 
      const email = socket.handshake.auth.email;
      
      socket.data.user = { uid, email };
      next();
    } catch (err) {
      next(new Error("Authentication error"));
    }
  });

  io.on("connection", (socket) => {
    console.log(`User connected: ${socket.data.user.uid}`);

    socket.on("update-location", async (payload) => {
      const { lat, lng, displayName } = payload;
      const userId = socket.data.user.uid;

      const event = {
        userId,
        displayName,
        lat,
        lng,
        timestamp: new Date().toISOString()
      };

      // Publish to Kafka
      await producer.send({
        topic: "location-updates",
        messages: [{ value: JSON.stringify(event) }]
      });
    });

    socket.on("disconnect", () => {
      console.log(`User disconnected: ${socket.data.user.uid}`);
    });
  });

  // --- VITE MIDDLEWARE ---
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*", (req, res) => {
      res.sendFile(path.join(distPath, "index.html"));
    });
  }

  httpServer.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
}

startServer();
