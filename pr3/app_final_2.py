import asyncio
import faust
import re
import time
import json
app = faust.App('test',broker='kafka://kafka1:9092', web_port=6066,debug=True, partitions=1,)

messages_topic = app.topic('messages', value_serializer='raw',partitions=1)
filtered_topic = app.topic('filtered_messages',partitions=1)
blocked_topic = app.topic('blocked_users',partitions=1)
commands_topic = app.topic('commands', value_serializer='raw',partitions=1)
user_block = app.Table('user_blocks', default=set,partitions=1)
word_block = app.Table('word_blocks', default=set,partitions=1)

initialized = False
initialized_word = False

def is_blocked(receiver_id: int, sender_id: int)->bool:#проверка на блокировку
    blocked_users = user_block.get(receiver_id)
    if blocked_users is None:
        blocked_users = set()
    return sender_id in blocked_users

def red_text(text:str) -> str:
    word_block_in = word_block.get(1)
    text = text.split(' ')
    text_red = []
    for word in text:
        if word not in word_block_in:
            text_red.append(word)
        else:
            text_red.append('*'*len(word))
    return ' '.join(text_red)

@app.agent(commands_topic)
async def process_commands(stream):
    global initialized,initialized_word
    async for command_value in stream:
        try:
            if isinstance(command_value,bytes):
                command_str = command_value.decode('utf-8')
            else:
                command_str = str(command_value)
            command = json.loads(command_str)
            cmd_type = command.get('type')
            cmd_id = command.get('command_id','unknown')
            #print(f'Тут все ок {command}')
            if cmd_type == 'block_user':
                blocker_id = command['blocker_id']
                blocked_id = command['blocked_id']
                current_blocks = user_block.get(blocker_id)
                if current_blocks is None:
                    current_blocks = set()
                if blocked_id in current_blocks:
                    print(f'user {blocked_id} already blocked ')
                else:
                    current_blocks.add(blocked_id)
                    user_block[blocker_id] = current_blocks
                    print(f' user{blocked_id} blocked')
            elif cmd_type == 'unblock_user':
                blocker_id = command['blocker_id']
                blocked_id = command['blocked_id']
                current_blocks = user_block.get(blocker_id)
                if current_blocks is None:
                    current_blocks = set()
                if blocked_id not in current_blocks:
                    print(f'user {blocked_id} not blocked ')
                else:
                    current_blocks.remove(blocked_id)
                    if current_blocks:
                        user_block[blocker_id] = current_blocks
                    else:
                        user_block.pop(blocker_id,None)
                    print(f' user{blocked_id} blocked')
            elif cmd_type == 'init_blocks':
                test_block = command.get('blocks',[])
                for blocker_id, blocked_id in test_block:
                    print(f'{blocker_id}, {blocked_id}')
                    if blocker_id not in user_block:
                        current_blocks = user_block.get(blocker_id)
                        if current_blocks is None:
                            current_blocks = set()
                        current_blocks.add(blocked_id)
                        user_block[blocker_id] = current_blocks
                        print(f'user {blocked_id} blocked')
                initialized = True
            elif cmd_type == 'init_word':
                test_block = command.get('blocks',[])
                for word_id in test_block:
                    print(f'{word_id}')
                    if word_id not in word_block:
                        current_block = word_block.get(1)
                        print(f'{current_block}')
                        if word_block is None:
                            current_blocks = set()
                        current_block.append(word_id)
                        word_block[1] = current_block
                        print(f'word {word_id} blocked')
                initialized_word = True
        except Exception as e:
            print(f'error field in message !!: {e}')

@app.agent(messages_topic) # осноная обработка
async def process_message(stream):
    global initialized, initialized_word
    while not initialized or not initialized_word:
        print('Ожидаем тестовые блокировки')
        await asyncio.sleep(10)
    async for message in stream:
        start_time = time.time()
        try:
            if isinstance(message,bytes):
                message_str = message.decode('utf-8')
            else:
                message_str = str(message)
            data = json.loads(message_str)
            required_fields = ['message_id','sender_id','receiver_id','text']
            missing_fieldes = [field for field in required_fields if field not in data]
            if missing_fieldes:
                print(f'пропушены поля в сообщения {data.get("message_id", "unknown")}: {missing_fieldes}')
                continue
            msg_id = data['message_id']
            sen_id = data['sender_id']
            rec_id = data['receiver_id']
            text = data['text']
            if is_blocked(rec_id,sen_id):
                print(f'Пользователь {rec_id} заблокирован')
                block_event = {'message_id': msg_id, 'sender_id': sen_id, 'receiver_id':rec_id, 'text_pr':text[:50],'reason':f'user {sen_id} blocked by {rec_id}'}
                await app.topic('blocked_users').send(value=json.dumps(block_event).encode('utf-8'))
                continue
            if not isinstance(data,dict):
                print(f"Invalid message format: {message[:50]}")
                continue
            process_message={**data,'processed_at':time.time(), 'processor':'faust-filter'}
            text = re.sub(r'([,.!?;:])(?![ \n])',r'\1 ',text)
            text = red_text(text)
            data['text'] = text
            await filtered_topic.send(value=json.dumps(data))
            print(f"Processed message: {data.get('message_id', 'unknown')}")
        except json.JSONDecodeError as e:
            print(f'Invalid json: {message[:50]}... Error:{e}')
        except Exception as e:
            print(f'error field in message: {e}')

@app.page('/')
async def home(self,request):
    return self.json({'status': 'running', 'app':'message filter system', 'time': time.time(),'endpoint': [
        {'path':'/','method': 'GET', 'description': 'Главная страница'},
        {'path': '/health', 'method': 'GET', 'description': 'Состояние состемы'},
        {'path': '/stats', 'method': 'GET', 'description': 'Статистика'},
        {'path': '/block/{user_id}', 'method': 'GET', 'description': 'Список заблокированных'}
#        {'path': '/block', 'method': 'POST', 'description': 'Добавить блокировку'},
#        {'path': '/unblock', 'method': 'POST', 'description': 'Удалить блокировку'},
         ]})
@app.page('/health')
async def health(self, request):
    return self.json({'status': 'healthy', 'timestamp': time.time(), 'kafka': 'kafka1:9092', 'topic': ['messages','filtered_messages']})
@app.page('/stats')
async def stats(self, request):
    return self.json({'status':'message-filter', 'uptime_seconds': time.time() - app.started_at if hasattr(app,'started_at') else 0,
                      'brocker_connected': True, 'timestamp': time.time()})
@app.page('/block/{user_id}')
async def blocklist(self, request, user_id: int):
    blocked = list(user_block.get(user_id) or set())
    return self.json({'user_id': user_id, 'blocker_users': blocked, 'block_count': len(blocked),
                      'timestamp': time.time()})
#@app.page('/test-send')
#async def test_send(self,request):
#    test_message = {'message_id':f'test_{int(time.time())}' , 'sender_id': 123, 'receiver_id':456, 'text': 'testing message number 1','timestamp': time.time()}
#    await messages_topic.send(value=json.dumps(test_message))
#    return self.json({'status':'sent','message':test_message,'note':'сообщение отправлено'})
@app.task
async def on_started():
    app.started_at = time.time()
    print(f'Sistem started')
    test_block = [(1,2),(1,3),(2,4)]
    init_command = {'type': 'init_blocks', 'blocks': test_block, 'timeatamp': time.time(), 'command_id': 'init_001'}
    await commands_topic.send(value=json.dumps(init_command))
    print(f'комнда отправлена')
    test_word = ['спам','блок','spam']
    init_command_2 = {'type': 'init_word', 'blocks': test_word, 'timeatamp': time.time(), 'command_id': 'init_002'}
    await commands_topic.send(value=json.dumps(init_command_2))
    print(f'комнда 2 отправлена')
if __name__ =='__main__':
    app.main()