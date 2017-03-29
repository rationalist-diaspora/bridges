import discord, asyncio, json, time, arrow
from rethinkdb import r
from core import SpQuery

loop = asyncio.get_event_loop()
client = discord.Client(loop=loop)
#r.set_loop_type("asyncio")

async def watch_db():
#    conn = await r.connect("localhost", 28015, db="rationalBridge")
#    messages = r.table("messages").changes(include_initial=True)['new_val'].run(conn)
    messages= SpQuery(r.table("messages").changes()['new_val'],"localhost", 28015, db="rationalBridge").run()
    async for message in messages:
        asyncio.sleep(4)
        print(message)
    

@client.event
async def on_ready():
    print('Logged in as')
    print(client.user.name)
    print(client.user.id)
    print('------')

utcify=lambda timeObject: arrow.get(timeObject).datetime

@client.event
async def on_message(message):
    conn = r.connect("localhost", 28015, db="rationalBridge")
    messageData={ #Convert our message into a format for json serialization.
        'timestamp':utcify(message.timestamp),
        'orig_timestamp':utcify(message.timestamp),
        'tts':message.tts,
        'discord_type':str(message.type),
        'author_id': "discord:{}".format(message.author.id),
        'server_id': "discord:{}".format(message.author.server.id),
        'content': message.content,
        'embeds': message.embeds,
        'room_id': "discord:{}".format(message.channel.id)
    }
    if message.edited_timestamp: 
        messageData['timestamp']=utcify(message.edited_timestamp)
    await r.table("messages").insert(messageData).run(conn)


def main():
    loop.create_task(watch_db())
    print("Starting discord listener...")
    client.run('MjgwODI3NDA4NzA0MzM5OTcw.C4PElw.E4iiC1OA86aC0bWzIGzqQsnncSI')


main()
