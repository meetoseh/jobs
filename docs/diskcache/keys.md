# diskcache

the keys we store locally on backend instances via diskcache

- `updater-lock-key` goes to a random token for the token we used to acquire
  the updater lock before shutting down to update. See the redis key
  `updates:{repo}:lock` for more information.
- `transcripts:{audio_sha512}`: Used to cache transcripts on audio files
  for which there is no corresponding content file.
