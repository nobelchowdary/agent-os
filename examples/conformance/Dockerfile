# Linux/amd64 child of the repository's pinned Python OCI index. Override only
# with a separately verified digest for an arm64 pilot, and record that digest.
ARG PYTHON_IMAGE=python@sha256:d76f585fb0d5107dd47143da5e61554fd9cae656a30f3470f815cb8475769b6a
FROM ${PYTHON_IMAGE}
COPY --chown=10001:10001 continuity_actor.py /app/continuity_actor.py
USER 10001:10001
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 STATE_DIR=/tmp/agentos-continuity PORT=8080
EXPOSE 8080
ENTRYPOINT ["python", "/app/continuity_actor.py"]
