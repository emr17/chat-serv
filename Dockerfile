FROM node:19.1.0

RUN apt-get update && apt-get install -y python3-pip
WORKDIR videoroom
COPY config.js app.js package.json ./
COPY public public
COPY server/ssl server/ssl
RUN npm install
RUN npm run build

CMD [ "node", "app.js" ]
