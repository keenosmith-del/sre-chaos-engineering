FROM docker:27.5.1-cli
RUN apk add --no-cache python3
COPY scripts/runner.py /runner.py
USER root
ENTRYPOINT ["python3","/runner.py"]
