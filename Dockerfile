FROM node:20-alpine

WORKDIR /app

COPY package.json ./
RUN npm ci --omit=dev

COPY server.js ./
COPY motion-detection ./

EXPOSE 7890
EXPOSE 7891
EXPOSE 7892/udp

CMD ["node", "server.js"]
