FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY server.js ./

EXPOSE 7890
EXPOSE 7891
EXPOSE 7892/udp

CMD ["node", "server.js"]
