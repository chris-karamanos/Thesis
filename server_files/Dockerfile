FROM node:20-slim

# Φάκελος εργασίας μέσα στο container
WORKDIR /app

# Μόνο τα package files για να κάνουμε cache το npm install
COPY package*.json ./

# Εγκατάσταση dependencies (production)
RUN npm install --only=production

# Αντιγραφή του κώδικα
COPY code ./code

# Το port που ακούει ο server ΜΕΣΑ στο container
ENV PORT=3000

# Εκκίνηση του server
CMD ["node", "code/index.js"]
