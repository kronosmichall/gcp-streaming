1. create pub/sub topic
2. create extractor that streams to pub/sub - deploy it as a Compute Engine VM
3. create built in pub/sub writer to bucket
4. create a cloud run function with Data Storage triggerer -> runs on each .json file input into cloud storage
by bucket writer
