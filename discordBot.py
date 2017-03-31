import discord, asyncio, json, time, arrow, os
from rethinkdb import r
from core import SpQuery
from collections import defaultdict

loop = asyncio.get_event_loop()
client = discord.Client(loop=loop)
#r.set_loop_type("asyncio")


lastUser=defaultdict(str)

async def watch_db():
#    conn = await r.connect("localhost", 28015, db="rationalBridge")
#    messages = r.table("messages").changes(include_initial=True)['new_val'].run(conn)
    messages= SpQuery(r.table("messages").changes()['new_val'],"localhost", 28015, db="rationalBridge").run()
    async for message in messages:
        serviceType, roomName=message['room_name'].split(":")
        if roomName.startswith("rd-") and serviceType=="discord":
            channels = []
            for server in client.servers:
                channels = channels+list(server.channels)
            for channel in channels:
                if channel.name==roomName and "discord:"+channel.id != message['room_id']:
                    msgStr="""```xml\n <{} {}={}>\n```\n""".format(message['author_name'].split(":")[1],message['server_name'],roomName)
                    msgStr=msgStr+message['content']
                    await client.send_message(channel, msgStr)
                    lastUser[message['room_name']]=message['author_id']
    

@client.event
async def on_ready():
    print('Logged in as')
    print(client.user.name)
    global clientId 
    clientId = client.user.id
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
        'author_name': "discord:{}".format(message.author.name),
        'server_id': "discord:{}".format(message.author.server.id),
        'server_name': "discord:{}".format(message.author.server.name),#This really should be rethinkd equievlent to a join, but I am lazy
        'content': message.content,
        'embeds': message.embeds,
        'room_id': "discord:{}".format(message.channel.id),
        'room_name': "discord:{}".format(message.channel.name),
    }
    if message.edited_timestamp:
        messageData['timestamp']=utcify(message.edited_timestamp)
    if messageData['author_id'] != "discord:"+clientId:
        r.table("messages").insert(messageData).run(conn)


def main():
    loop.create_task(watch_db())
    print("Starting discord listener...")
    client.run(os.environ['discord_key'])


main()
