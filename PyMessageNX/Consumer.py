from MessageNX import MessageNX

mnx = MessageNX()

def processor(job):
  print(job)


mnx.set_channel("appId")


mnx.consume(process_callback=processor)