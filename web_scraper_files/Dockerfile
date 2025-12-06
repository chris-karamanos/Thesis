# Βάση: επίσημο Playwright image με Python και browsers εγκατεστημένα
FROM mcr.microsoft.com/playwright/python:v1.56.0-jammy

# Λιγότερο "σκουπίδι" και πιο καθαρά logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Φάκελος εργασίας μέσα στο container
WORKDIR /app

# 1. Περνάμε τα requirements και τα εγκαθιστούμε
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Περνάμε ΟΛΟ τον κώδικα του scraper
COPY . .

# 3. Entrypoint του container:
#    Αν το κύριο script σου ΔΕΝ λέγεται main.py, άλλαξέ το αντίστοιχα.
CMD ["sleep", "infinity"]
