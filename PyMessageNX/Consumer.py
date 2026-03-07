from MessageNX import MessageNX

mnx = MessageNX()

def processor(job):
  print(job['name'])


mnx.set_channel("141983614")

mnx.consume(process_callback=processor)