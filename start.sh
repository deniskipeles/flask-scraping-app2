#!/bin/bash

# Start RQ worker for the 'groq' and 'post' queues
rq worker -u redis://red-cplhati1hbls73ef82gg:6379 groq post &

# Start Gunicorn to serve the Flask app
exec gunicorn -w 4 -b :5000 app:app
