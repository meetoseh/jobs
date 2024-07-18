# diskcache

the keys we store locally on backend instances via diskcache

- `updater-lock-key` goes to a random token for the token we used to acquire
  the updater lock before shutting down to update. See the redis key
  `updates:{repo}:lock` for more information.
- `transcripts:{audio_sha512}`: Used to cache transcripts on audio files
  for which there is no corresponding content file.
- `transcripts:byuid:{uid}`: Used to cache transcripts by their database
  stable external identifier.
- `journey_embeddings`: Used to cache the current journey embeddings. Always
  has an expiry set, usually around 3am PST.
  Formatted as (uint32, blob, uint64, blob), where the numbers are lengths,
  the first blob is the metadata from the corresponding redis key and the
  second blob is the contents of the referenced s3 object.
