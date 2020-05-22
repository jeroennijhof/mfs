FROM alpine
WORKDIR /app
COPY mfsd mfsd
ENTRYPOINT ["/app/mfsd"]
CMD ["-h"]
