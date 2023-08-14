# backup_1c_base - Программа создания резервных копий информационных баз 1С на Linux

## Установка

Необходимые пакеты:

```
sudo apt install nfs-common
sudo apt install python3-dialog
```

Резервные копии располагаются на NFS сетевых ресурсах.

На сервере приложений 1С должна быть запущена утилита ras в режиме демона:

```shell
/opt/1cv8/x86_64/ras cluster --daemon --port=1545
```

Запуск создания резервных копий разделяем на 3 части:

1. Запрещаем выполнение фоновых заданий и ожидаем завершения уже запущенных
2. Запуск создания резервных копий
3. Востанавливаем разрешение выполнения фоновых заданий

В настройках CRON эти этапы выглядят как:
```shell
# m h  dom mon dow   command
0 22 * * * python3 /home/user/prg/backup_1c_base/set_1c_scheduled_jobs.py --debug --settings=/home/user/prg/backup_1c_base/settings.ini --on 1>/home/user/prg/backup_1c_base/stdout_scheduled_jobs_on.log 2>/home/user/prg/backup_1c_base/error_scheduled_jobs_on.log
0 2 * * * export DISPLAY=:0; python3 /home/user/prg/backup_1c_base/backup_1c_base.py --debug --settings=/home/user/prg/backup_1c_base/settings.ini 1>/home/user/prg/backup_1c_base/stdout_backup.log 2>/home/user/prg/backup_1c_base/error_backup.log
0 5 * * * python3 /home/user/prg/backup_1c_base/set_1c_scheduled_jobs.py --debug --settings=/home/user/prg/backup_1c_base/settings.ini --off 1>/home/user/prg/backup_1c_base/stdout_scheduled_jobs_off.log 2>/home/user/prg/backup_1c_base/error_scheduled_jobs_off.log
```

> Для запуска графических оконных приложений в cron необходимо указать **export DISPLAY=:0**


## Настройка и использование

Запускать можно на клиентской машине с той же версией 1С что и на сервере.
Параметры запуска исполняемых скриптов можно по ключу **--help** --
Например:

```shell
python3 backup_1c_base.py --help
python3 set_1c_sheduled_jobs.py --help
```

## Обновление версии 1С

1. Обновить версию сервера 1С
2. Обновить версию клиента 1С (Версия клиента должна устанавливаться с набором инструментов администрирования)
3. В настройках **settings.ini** необходимо поменять путь к клиентской 1с на новый с учетом версии
4. Проверить на клиенте соответствие ip адрес - имя сервера в файле **/etc/hosts**
