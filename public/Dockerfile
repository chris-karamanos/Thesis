FROM node:20-slim

WORKDIR /app

# install deps based on backend/package.json
COPY backend/package*.json ./backend/
RUN cd backend && npm install

# copy source code 
COPY backend/code ./backend/code
COPY frontend ./frontend

ENV PORT=3000
EXPOSE 3000

CMD ["node", "backend/code/index.js"]
