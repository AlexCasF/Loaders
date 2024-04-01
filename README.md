# Loaders
different data/metadata loaders to connect to RAG pipelines such as the ones provided with langchain, embedchain, llamaindex...

Each loader class consists of several detailled methods and a main loading method returning a list of dictionaries like:

  {
  'text':TEXT_STRING, 
  'metadata':{METADATA_DICT}
  },
  {
  'text':TEXT_STRING, 
  'metadata':{METADATA_DICT}
  },
  {
  'text':TEXT_STRING, 
  'metadata':{METADATA_DICT}
  },
  ...
  ...


    
