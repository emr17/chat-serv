import { config } from "./config.js";
import express from "express";
const app = express();
import Cryptr from "cryptr";
const cryptr = new Cryptr("myTotallySecretKey");
//import { db, Message } from "./mongo-connection.js";

import https from "httpolyglot";
import fs from "fs";
import path, { format } from "path";
const __dirname = path.resolve();

import { Server } from "socket.io";
import mediasoup from "mediasoup";

const urlPath = "/mediasoup/";

app.get("*", (req, res, next) => {
  let path = urlPath;
  if (req.path.indexOf(path) == 0 && req.path.length > path.length)
    return next();
  res.send("Specify a room name. ie " + urlPath + "room1/");
});

app.use(`${urlPath}:room`, express.static(path.join(__dirname, "public")));

const options = {
  key: fs.readFileSync(config.ssl.keyPath, "utf-8"),
  cert: fs.readFileSync(config.ssl.certPath, "utf-8"),
};

const httpsServer = https.createServer(options, app);
httpsServer.listen(config.appPort, () => {
  console.log("Listening on port: " + config.appPort);
});

app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  next();
});

/*app.get("/mediasoup/:room/getMessages", (req, res) => {
  var messages;
  console.log("dsfsdfdsf");
  console.log(req.params.room);
  Message.find(
    { roomId: req.params.room },
    "sender roomId message date",
    (err, athletes) => {
      if (err) return handleError(err);

      messages = athletes;
      messages.map(
        (message) => (message.message = cryptr.decrypt(message.message))
      );

      res.send(messages);
    }
  );
});*/

const io = new Server(httpsServer);

const connections = io.of("/mediasoup");

let workers = [];
let nextWorker = 0;
let rooms = {};
let peers = {};
let transports = [];
let producers = [];
let consumers = [];
let currentRoom = null;

const createWorkers = async () => {
  let minPort = config.rtcMinPort;
  let workerPortCount = Math.floor(
    (config.rtcMaxPort - config.rtcMinPort) / config.maxWorkers
  );
  for (let i = 0; i < config.maxWorkers; i++) {
    let maxPort = minPort + workerPortCount - 1;
    let worker = await mediasoup.createWorker({
      rtcMinPort: minPort,
      rtcMaxPort: maxPort,
    });

    console.log(`Worker ${i} pid ${worker.pid}`);

    worker.on("died", (error) => {
      console.error("mediasoup worker has died");
      setTimeout(() => process.exit(1), 2000);
      // TODO need to remove from workers and possibly update nextWorker
    });

    workers.push(worker);
    minPort = maxPort + 1;
  }
  nextWorker = 0;
};

createWorkers();

connections.on("connection", async (socket) => {
  console.log(`Peer connected: ${socket.id}`);
  socket.emit("connection-success", {
    socketId: socket.id,
  });

  

  const removeItems = (items, socketId, type) => {
    items.forEach((item) => {
      if (item.socketId === socket.id) {
        item[type].close();
      }
    });
    items = items.filter((item) => item.socketId !== socket.id);
    return items;
  };

  socket.on("disconnect", () => {
    console.log(`Peer disconnected ${socket.id}`);
    consumers = removeItems(consumers, socket.id, "consumer");
    producers = removeItems(producers, socket.id, "producer");
    transports = removeItems(transports, socket.id, "transport");

    const { roomName } = peers[socket.id];
    delete peers[socket.id];

    rooms[roomName] = {
      router: rooms[roomName].router,
      peers: rooms[roomName].peers.filter((socketId) => socketId !== socket.id),
    };
  });

  socket.on("joinRoom", async ({ roomName }, callback) => {
    const router1 = await createRoom(roomName, socket.id);
    currentRoom = roomName;
    socket.join(currentRoom, function () {
      console.log(socket.id + " now in rooms ", socket.rooms);
    });

    peers[socket.id] = {
      socket,
      roomName,
      transports: [],
      producers: [],
      consumers: [],
      peerDetails: {
        name: "",
        isAdmin: false,
      },
    };

    const rtpCapabilities = router1.rtpCapabilities;

    callback({ rtpCapabilities });
  });

  const createRoom = async (roomName, socketId) => {
    let router1;
    let peers = [];
    if (rooms[roomName]) {
      router1 = rooms[roomName].router;
      peers = rooms[roomName].peers || [];
    } else {
      const thisWorker = nextWorker;
      nextWorker = (nextWorker + 1) % config.maxWorkers;
      router1 = await workers[thisWorker].createRouter(config.routerOptions);
      console.log(`New router ID: ${router1.id} on worker: ${thisWorker}`);
    }

    rooms[roomName] = {
      router: router1,
      peers: [...peers, socketId],
    };

    return router1;
  };

  socket.on("createWebRtcTransport", async ({ consumer }, callback) => {
    const roomName = peers[socket.id].roomName;
    const router = rooms[roomName].router;

    createWebRtcTransport(router).then(
      (transport) => {
        callback({
          params: {
            id: transport.id,
            iceParameters: transport.iceParameters,
            iceCandidates: transport.iceCandidates,
            dtlsParameters: transport.dtlsParameters,
          },
        });

        addTransport(transport, roomName, consumer);
      },
      (error) => {
        console.log(error);
      }
    );
  });

  const addTransport = (transport, roomName, consumer) => {
    transports = [
      ...transports,
      { socketId: socket.id, transport, roomName, consumer },
    ];

    peers[socket.id] = {
      ...peers[socket.id],
      transports: [...peers[socket.id].transports, transport.id],
    };
  };

  const addProducer = (producer, roomName) => {
    producers = [...producers, { socketId: socket.id, producer, roomName }];

    peers[socket.id] = {
      ...peers[socket.id],
      producers: [...peers[socket.id].producers, producer.id],
    };
  };

  const addConsumer = (consumer, roomName) => {
    consumers = [...consumers, { socketId: socket.id, consumer, roomName }];

    peers[socket.id] = {
      ...peers[socket.id],
      consumers: [...peers[socket.id].consumers, consumer.id],
    };
  };

  socket.on("getProducers", (callback) => {
    const { roomName } = peers[socket.id];

    let producerList = [];
    producers.forEach((producerData) => {
      if (
        producerData.socketId !== socket.id &&
        producerData.roomName === roomName
      ) {
        producerList = [...producerList, producerData.producer.id];
      }
    });

    callback(producerList);
  });

  const informConsumers = (roomName, socketId, id) => {
    console.log(`${socketId}:${id} just joined ${roomName}`);
    producers.forEach((producerData) => {
      if (
        producerData.socketId !== socketId &&
        producerData.roomName === roomName
      ) {
        const producerSocket = peers[producerData.socketId].socket;
        producerSocket.emit("new-producer", { producerId: id });
      }
    });
  };

  const getTransport = (socketId) => {
    const [producerTransport] = transports.filter(
      (transport) => transport.socketId === socketId && !transport.consumer
    );
    return producerTransport.transport;
  };

  socket.on("transport-connect", async ({ dtlsParameters }) => {
    getTransport(socket.id).connect({ dtlsParameters });
  });

  socket.on(
    "transport-produce",
    async ({ kind, rtpParameters, addData }, callback) => {
      const producer = await getTransport(socket.id).produce({
        kind,
        rtpParameters,
      });

      const { roomName } = peers[socket.id];

      addProducer(producer, roomName);
      console.log("New producer ID: ", producer.id, producer.kind);

      informConsumers(roomName, socket.id, producer.id);

      producer.on("transportclose", () => {
        console.log(`Transport for producer ${producer.id} closed`);
        producer.close();
      });

      callback({
        id: producer.id,
        producersExist: producers.length > 1 ? true : false,
      });
    }
  );

  socket.on(
    "transport-recv-connect",
    async ({ dtlsParameters, serverConsumerTransportId }) => {
      const consumerTransport = transports.find(
        (transportData) =>
          transportData.consumer &&
          transportData.transport.id == serverConsumerTransportId
      ).transport;
      await consumerTransport.connect({ dtlsParameters });
    }
  );

  socket.on(
    "consume",
    async (
      { rtpCapabilities, remoteProducerId, serverConsumerTransportId },
      callback
    ) => {
      try {
        const { roomName } = peers[socket.id];
        const router = rooms[roomName].router;
        let consumerTransport = transports.find(
          (transportData) =>
            transportData.consumer &&
            transportData.transport.id == serverConsumerTransportId
        ).transport;

        if (
          router.canConsume({ producerId: remoteProducerId, rtpCapabilities })
        ) {
          const consumer = await consumerTransport.consume({
            producerId: remoteProducerId,
            rtpCapabilities,
            paused: true,
          });

          consumer.on("transportclose", () => {
            console.log(`Transport close from consumer ${consumer.id}`);
          });

          consumer.on("producerclose", () => {
            console.log(
              `Producer ${remoteProducerId} of consumer ${consumer.id} closed`
            );
            socket.emit("producer-closed", { remoteProducerId });

            consumerTransport.close({});
            transports = transports.filter(
              (transportData) =>
                transportData.transport.id !== consumerTransport.id
            );
            consumer.close();
            consumers = consumers.filter(
              (consumerData) => consumerData.consumer.id !== consumer.id
            );
          });

          addConsumer(consumer, roomName);

          const params = {
            id: consumer.id,
            producerId: remoteProducerId,
            kind: consumer.kind,
            rtpParameters: consumer.rtpParameters,
            serverConsumerId: consumer.id,
          };

          callback({ params });
        }
      } catch (error) {
        console.log(error.message);
        callback({
          params: {
            error: error,
          },
        });
      }
    }
  );

  socket.on("consumer-resume", async ({ serverConsumerId }) => {
    const { consumer } = consumers.find(
      (consumerData) => consumerData.consumer.id === serverConsumerId
    );
    await consumer.resume();
  });
});

const createWebRtcTransport = async (router) => {
  return new Promise(async (resolve, reject) => {
    try {
      let transport = await router.createWebRtcTransport(
        config.webRtcTransportOptions
      );

      transport.on("dtlsstatechange", (dtlsState) => {
        if (dtlsState === "closed") {
          transport.close();
        }
      });

      transport.on("close", () => {
        console.log("Transport closed");
      });

      resolve(transport);
    } catch (error) {
      reject(error);
    }
  });
};
