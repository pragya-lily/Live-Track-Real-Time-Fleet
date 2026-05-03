#  LiveTrack – Real-Time Location Tracking System

##  Project Overview

LiveTrack is a real-time location tracking web application where authenticated users can share their live location and view other users moving on a map.

The system is built using an event-driven architecture powered by **Kafka**, ensuring scalability and high-throughput handling of location updates.

---

##  Features

*  Secure authentication using OIDC / OAuth 2.0
*  Real-time location sharing
*  Live map with moving user markers
*  Event-driven architecture using Kafka
*  WebSocket-based real-time communication
*  Multiple Kafka consumers (socket + database processing)
*  Handles disconnects and stale users
*  Duplicate and invalid event handling

---

##  Screenshots

###  Home Page

<!-- Add your homepage screenshot below -->



---

###  Login Page

<!-- Add your login page screenshot below -->



---

##  Tech Stack

### Frontend

* HTML, CSS, JavaScript
* Map Integration (Google Maps / Leaflet)

### Backend

* Node.js
* Express.js
* Socket.IO

### Messaging & Streaming

* Apache Kafka

### Authentication

* OIDC / OAuth 2.0

### Database

* (Mention your DB or write "Simulated Storage")

---

##  System Architecture Flow

1. User logs in via OAuth/OIDC
2. User grants location permission
3. Frontend sends location via WebSocket (Socket.IO)
4. Server receives location and publishes event to Kafka
5. Kafka consumers process events:

   * Consumer 1 → Broadcasts updates via sockets
   * Consumer 2 → Stores location data in DB
6. Frontend updates map markers in real-time

---

##  Socket Event Flow

* `connect` → User connects with authentication
* `send-location` → Client sends location
* `receive-location` → Broadcast updated location to all users
* `disconnect` → Remove user from active map

---

##  Kafka Event Flow

* Producer: Backend server publishes location events
* Topic: `location-updates`
* Consumers:

  * Socket Broadcaster → Sends updates to clients
  * Database Processor → Stores location history

---

##  Setup Instructions

### 1. Clone Repository

```bash
git clone https://github.com/your-username/livetrack.git
cd livetrack
```

### 2. Install Dependencies

```bash
npm install
```

### 3. Setup Environment Variables

Create a `.env` file:

```env
PORT=5000
KAFKA_BROKER=localhost:9092
OAUTH_CLIENT_ID=your_client_id
OAUTH_CLIENT_SECRET=your_secret
```

---

### 4. Start Kafka

Make sure Kafka and Zookeeper are running.

---

### 5. Run Project

```bash
npm run dev
```

---

##  OIDC Authentication Setup

* Register your app with an OAuth provider (Google / Auth0)
* Add redirect URI
* Use client ID & secret in `.env`
* Integrate authentication in backend and socket connection

---

##  Demo Video


```
https://youtube.com/your-demo-link
```

---

##  Assumptions & Limitations

* Location updates are sent at fixed intervals
* Kafka is running locally (or configured manually)
* Map accuracy depends on browser GPS
* No advanced clustering for large-scale users
* Basic error handling implemented

---


##  Author

**Pragya Rajput**

---

##  If you like this project

Give it a ⭐ on GitHub!
