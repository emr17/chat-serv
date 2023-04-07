const io = require("socket.io-client");
const mediasoupClient = require("mediasoup-client");

const roomName = window.location.pathname.split("/")[2];

const socket = io("/mediasoup");

const videoGrid = document.getElementById("video-grid");
const showChat = document.querySelector("#showChat");
const backBtn = document.querySelector(".header__back");

backBtn.addEventListener("click", () => {
  document.querySelector(".main__left").style.display = "flex";
  document.querySelector(".main__left").style.flex = "1";
  document.querySelector(".main__right").style.display = "none";
  document.querySelector(".header__back").style.display = "none";
});

showChat.addEventListener("click", () => {
  document.querySelector(".main__right").style.display = "flex";
  document.querySelector(".main__right").style.flex = "1";
  document.querySelector(".main__left").style.display = "none";
  document.querySelector(".header__back").style.display = "block";
});

const user = prompt("Enter your name");

const defaultVideoParams = {
  // mediasoup params
  encodings: [
    { scaleResolutionDownBy: 4, maxBitrate: 5_000_000 },
    { scaleResolutionDownBy: 2, maxBitrate: 10_000_000 },
    { scaleResolutionDownBy: 1, maxBitrate: 50_000_000 },
  ],
  codecOptions: {
    videoGoogleStartBitrate: 100_000, // Başlangıç bit hızını düşürün
  },
};

let device;
let rtpCapabilities;
let producerTransport;
let consumerTransports = [];
let audioProducer;
let videoProducer;
let consumer;
let isProducer = false;

let audioParams;
let videoParams = defaultVideoParams;
let consumingTransports = [];

socket.on("connection-success", ({ socketId }) => {
  console.log(socketId);
  getLocalStream();
});

const streamSuccess = async (stream) => {
  localVideo.srcObject = stream;

  audioParams = { track: stream.getAudioTracks()[0], ...audioParams };
  videoParams = { track: stream.getVideoTracks()[0], ...videoParams };

  joinRoom();
};

const joinRoom = () => {
  socket.emit("joinRoom", { roomName }, (data) => {
    console.log(`Router RTP Capabilities... ${data.rtpCapabilities}`);
    rtpCapabilities = data.rtpCapabilities;
    createDevice();
  });
};

const getLocalStream = () => {
  const mediaOptions = {
    audio: true,
    video: {
      width: 1920,
      height: 1080,
    },
  };
  navigator.mediaDevices
    .getUserMedia(mediaOptions)
    .then(streamSuccess)
    .catch((error) => {
      console.log(error.message);
    });
};

const createDevice = async () => {
  try {
    device = new mediasoupClient.Device();

    await device.load({
      routerRtpCapabilities: rtpCapabilities,
    });

    console.log("RTP Capabilities", rtpCapabilities);

    createSendTransport();
  } catch (error) {
    console.log(error);
    if (error.name === "UnsupportedError")
      console.warn("browser not supported");
  }
};

const createSendTransport = () => {
  socket.emit("createWebRtcTransport", { consumer: false }, ({ params }) => {
    if (params.error) {
      console.log(params.error);
      return;
    }

    console.log(params);

    producerTransport = device.createSendTransport(params);

    producerTransport.on(
      "connect",
      async ({ dtlsParameters }, callback, errback) => {
        try {
          await socket.emit("transport-connect", {
            dtlsParameters,
          });

          callback();
        } catch (error) {
          errback(error);
        }
      }
    );

    producerTransport.on("produce", async (parameters, callback, errback) => {
      console.log(parameters);

      try {
        await socket.emit(
          "transport-produce",
          {
            kind: parameters.kind,
            rtpParameters: parameters.rtpParameters,
            appData: parameters.appData,
          },
          ({ id, producersExist }) => {
            callback({ id });
            if (producersExist) getProducers();
          }
        );
      } catch (error) {
        errback(error);
      }
    });

    connectSendTransport();
  });
};

const connectSendTransport = async () => {
  audioProducer = await producerTransport.produce(audioParams);
  videoProducer = await producerTransport.produce(videoParams);

  audioProducer.on("trackended", () => {
    console.log("audio track ended");
  });

  audioProducer.on("transportclose", () => {
    console.log("audio transport ended");
  });

  videoProducer.on("trackended", () => {
    console.log("video track ended");
  });

  videoProducer.on("transportclose", () => {
    console.log("video transport ended");
  });
};

const signalNewConsumerTransport = async (remoteProducerId) => {
  if (consumingTransports.includes(remoteProducerId)) return;
  consumingTransports.push(remoteProducerId);

  await socket.emit(
    "createWebRtcTransport",
    { consumer: true },
    ({ params }) => {
      if (params.error) {
        console.log(params.error);
        return;
      }
      console.log(`params... ${params}`);

      let consumerTransport;
      try {
        consumerTransport = device.createRecvTransport(params);
      } catch (error) {
        console.log(error);
        return;
      }

      consumerTransport.on(
        "connect",
        async ({ dtlsParameters }, callback, errback) => {
          try {
            await socket.emit("transport-recv-connect", {
              dtlsParameters,
              serverConsumerTransportId: params.id,
            });

            callback();
          } catch (error) {
            errback(error);
          }
        }
      );

      connectRecvTransport(consumerTransport, remoteProducerId, params.id);
    }
  );
};

socket.on("new-producer", ({ producerId }) =>
  signalNewConsumerTransport(producerId)
);

const getProducers = () => {
  socket.emit("getProducers", (producerIds) => {
    console.log(producerIds);
    producerIds.forEach(signalNewConsumerTransport);
  });
};

const connectRecvTransport = async (
  consumerTransport,
  remoteProducerId,
  serverConsumerTransportId
) => {
  await socket.emit(
    "consume",
    {
      rtpCapabilities: device.rtpCapabilities,
      remoteProducerId,
      serverConsumerTransportId,
    },
    async ({ params }) => {
      if (params.error) {
        console.log("cannot consume");
        return;
      }

      console.log(`consumer params ${params}`);
      const consumer = await consumerTransport.consume({
        id: params.id,
        producerId: params.producerId,
        kind: params.kind,
        rtpParameters: params.rtpParameters,
      });

      consumerTransports = [
        ...consumerTransports,
        {
          consumerTransport,
          serverConsumerTransportId: params.id,
          producerId: remoteProducerId,
          consumer,
        },
      ];

      const newElem = document.createElement("div");
      newElem.setAttribute("id", `td-${remoteProducerId}`);

      if (params.kind == "audio") {
        newElem.innerHTML =
          '<audio id="' + remoteProducerId + '" autoplay></audio>';
      } else {
        newElem.setAttribute("class", "remoteVideo");
        newElem.innerHTML =
          '<video id="' +
          remoteProducerId +
          '" autoplay class="video"></video>';
      }
      videogrid.appendChild(newElem);

      const { track } = consumer;

      document.getElementById(remoteProducerId).srcObject = new MediaStream([
        track,
      ]);

      socket.emit("consumer-resume", {
        serverConsumerId: params.serverConsumerId,
      });
    }
  );
};

socket.on("producer-closed", ({ remoteProducerId }) => {
  const producerToClose = consumerTransports.find(
    (transportData) => transportData.producerId === remoteProducerId
  );
  producerToClose.consumerTransport.close();
  producerToClose.consumer.close();

  consumerTransports = consumerTransports.filter(
    (transportData) => transportData.producerId !== remoteProducerId
  );

  videogrid.removeChild(document.getElementById(`td-${remoteProducerId}`));
});

let messages = document.getElementById("messages");

socket.on("createMessage", (data) => {
  console.log("aaaaaaaaaaaaaaaaaaa mesaj geldi");
  messagegrid.innerHTML =
    messagegrid.innerHTML +
    `<div class="message">
		  <b><i class="far fa-user-circle"></i> <span> ${
        (data.userName === user ? "me" : data.userName) + " | "
      }</span> </b>
		  <span>${data.message}</span>
	  </div>`;
});

send.addEventListener("click", (e) => {
  if (chat_message.value.length !== 0) {
    socket.emit("message", { message: chat_message.value, roomName: roomName });
    console.log("fsd");
    chat_message.value = "";
  }
});

getInitalMessages = () => {
  fetch("https://localhost:4000/mediasoup/" + roomName + "/getMessages")
    .then((response) => response.json())
    .then((data) => {
      data.map((message) => {
        messages.innerHTML =
          messages.innerHTML +
          `<div class="message">
            <b><i class="far fa-user-circle"></i> <span> ${
              (message.sender === user ? "me" : message.sender) +
              " | " +
              message.date
            }</span> </b>
            <span>${message.message}</span>
        </div>`;
      });
    });
};
