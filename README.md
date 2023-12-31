# RepotWebsiteCarbaz

Скрипт для объединения отчетов из разных баз.

Для работы с данными, таблицами и эксель файлами используется библиотека [pandas](https://pypi.org/project/pandas/)

Для работы с файлами на удалённом сервере используется smbclient из библиотеки [smbprotocol](https://pypi.org/project/smbprotocol/)

Для работы с файлами в папке со скриптом используем модуль `os`

Для построения графиков используется библиотека [matplotlib](https://pypi.org/project/matplotlib/) 
### Описание

------------
1. Считываем данные из файлов с отчетами из разных баз и из файла с отчетом по отправки СМС сообщений с сервиса Beeline.
Файлы расположены на сервере. Для считывания файлов с сервера используем библиотеку smbclient
2. Считываем данные за прошлые периоды из фалов находящихся в папке со скриптом
3. Добавляем считанные данные из отчетов и сохраняем файлы с добавленными данными в папку со скриптом.
4. На основании общих данных за все периоды формируем конечные файлы с отчетом за все периоды.
5. Отправляем письмо с отчетами и в теле письма отправляем общие данные по количеству строк за прошедший месяц.
- Формируем данные для письма в основном скрипте.
- Отправляем письмо с помощью скрипта **send_mail.py**

### Принцип работы send_mail.py

------------

В данном файле прописана одна функция **send** с параметром `message` в виде словаря:

```python
message = {
        'Subject': str, # Тема письма
        'email_content': str, # Текст письма в виде html
        'To': list, # Список с адресами получателей,
        'File_name': list or str, # Наименование файлов(а), которое будет отображаться в письме
        'Temp_file': list or str  # Наименование файлов(а), которое будет добавлено к письму
        }
```
Если необходимо отправить письмо без вложений файлов, то в значение ключей `File_name` и `Temp_file` необходимо передать 
пустые строки `''`.

### Содержание файла config.py

------------

Данные для доступа размещаем в файл config.py
Данные для доступа к папкам на сервере:
````python
LOCAL_PATH = {
    'PATH_REPORT': r'\\server(IP or name)\полный путь к папке с отчетами на сервере',
    'USER': r'domain\User',
    'PSW': 'password'
}
````
Данные доступа к почтовому ящику для отправки:
```python
EMAIL_CONFIG = {
    'FROM': 'login',
    'PSW': 'password'
}
```
Адреса электронных почт для отправки:
````python
TO_EMAILS = {
    'TO_CORRECT': list or str, # Список электронных почт для отправки результатов работы программы
    'TO_ERROR': list or str # Список электронных почт для отправки сообщений об ошибках
}
````
### Примечание 

------------
Для логирования используется библиотека [logguru](https://loguru.readthedocs.io/en/stable/overview.html)
Наименование лог файла прописывается в файле config.py в переменную `FILE_NAME_CONFIG`
