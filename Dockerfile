FROM clojure:temurin-21-lein-2.11.2-noble

RUN apt-get update && \
    apt-get install -y --no-install-recommends gnuplot graphviz && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf "$(which java)" /usr/bin/java

WORKDIR /app

# Cache local-node deps first
COPY local-node/project.clj local-node/project.clj
RUN cd local-node && lein deps

# Cache test harness deps
COPY project.clj project.clj
RUN lein deps

# Copy source
COPY . .

# Build local-node uberjar
RUN cd local-node && lein uberjar

ENTRYPOINT ["lein"]
CMD ["run", "test", "--time-limit", "10"]
