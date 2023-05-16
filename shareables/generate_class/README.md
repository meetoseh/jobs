# Generate Class

The goal of this module is to generate an entirely new audio file, title, description,
and prompt that form the core of a new journey.

The general process is as follows:

- select 2-5 classes which all have one common tag and are all in the same category
  (e.g., breathwork + open)
- combining class transcripts, titles, and descriptions ask for a new transcript+title+description
  from chatgpt using the fake instructor "Anthony Chip"
- Turn that transcript into timestamps using chatgpt
- Turn the transcript into audio using play.ht
- Add background music (??)
- Turn it into a class

When testing manually it's generally best to describe the source classes directly
rather than pulling them from the database, as fetching from the actual database
can only be done on production. To do this, create `tmp/generate_class.json` in
the following format:

```json
{
  "category": "string",
  "emotion": "string",
  "classes": [
    {
      "title": "string",
      "description": "string",
      "prompt": {
        "style": "word",
        "text": "string",
        "options": ["string"]
      },
      "audio_path": "string"
    }
  ]
}
```
