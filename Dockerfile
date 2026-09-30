FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN python -m pip install --no-cache-dir . && chmod -R a+rX /app
USER 1001
EXPOSE 8765
ENTRYPOINT ["hivemind-server"]
CMD ["--host", "0.0.0.0", "--port", "8765"]
