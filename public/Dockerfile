FROM node:20-slim

# Φάκελος εργασίας μέσα στο container
WORKDIR /app

# Μόνο τα package files για να κάνουμε cache το npm install
COPY backend/package*.json ./backend/

# Εγκατάσταση dependencies (production)
RUN cd backend && npm install --only=production

# Αντιγραφή του κώδικα
COPY backend/code ./backend/code
COPY frontend ./frontend

# Το port που ακούει ο server ΜΕΣΑ στο container
ENV PORT=3000

# Εκκίνηση του server
CMD ["node", "backend/code/index.js"]
