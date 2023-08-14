#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт автоматического создания бэкапа баз 1С под Linux.

Необходимые пакеты:
sudo apt install nfs-common
sudo apt install python3-dialog

Дополнительная информация:
Запускается только на компьютере с установленным клиентом 1С.
На сервере приложений 1С должна быть запущена утилита ras в режиме демона:
/opt/1cv8/x86_64/ras cluster --daemon --port=1545

ВАЖНО:
Для запуска графических оконных приложений в cron необходимо указать export DISPLAY=:0
Например:
# m h  dom mon dow   command
0 10 * * * export DISPLAY=:0; python3 /home/user/prg/backup_1c_base/backup_1c_base.py --debug --settings=/home/user/prg/backup_1c_base/settings.ini 1>/home/user/prg/backup_1c_base/stdout.log 2>/home/user/prg/backup_1c_base/error.log

Запуск:

        python3 backup_1c_base.py [параметры командной строки]

Пример запуска:
        python3 backup_1c_base.py --debug --settings=/home/user/prg/backup_1c_base/settings.ini

Параметры командной строки:

    [Помощь и отладка]
        --help|-h|-?        Помощь
        --version|-v        Версия программы
        --debug|-d          Включить сообщения отладки

    [Основные опции]
        --settings=         Явное указание файла настроек.
                            Если не указывается, то берется по умолчанию файл settings.ini.
        --dlg               Выполнение программы в диалоговом режиме
        --host=             Сервер 1С
        --port=             Порт утилиты ras
                            На сервере приложений 1С должна быть запущена утилита ras в режиме демона:
                            /opt/1cv8/x86_64/ras --daemon cluster --port=1545
        --name=             Наименование базы
        --path_1c=          Путь к установленным программам 1С
                            Например: /opt/1cv8/x86_64
        --backup=           Указание ресурса для хранения бэкапов
                            Например:
                            nfs://workgroup;user:password@server:/share/folder
        --delete            Удалить не актуальные бэкапы
        --actual_period=    Временной период актуальности бэкапов
                            Например: 0000-00-10
        --admin=            Администратор 1С
        --password=         Пароль администратора 1С
        --scheduled_jobs    Вкл./Откл. запуск регламентных заданий в процессе создания резервной копии

        --report_enabled    Вкл. отправку отчета
        --report_from=      Адрес с которого отсылается письмо
        --report_to=        Адрес получателя письма
        --report_subject=   Тема письма
        --smtp_server=      SMTP сервер
        --smtp_server_port= Порт SMTP сервера
        --smtp_login=       Логин на SMTP сервере
        --smtp_password=    Парот на SMTP сервере
"""

import sys
import getopt
import locale
import os
import os.path
import traceback
import datetime
import tempfile
import subprocess
import urllib.parse
import time
import shutil

try:
    import configparser
except ImportError:
    print('Import error configparser')

try:
    import dialog
except ImportError:
    print('Import error pythondialog. Install: sudo apt install python3-dialog')

import email.mime.multipart
import email.mime.text
import email.mime.base
import email.utils
import email.encoders
import smtplib

__version__ = (0, 0, 7, 1)

# Режим отладки
DEBUG_MODE = False

# Диалоговый режим работы программы
DIALOG_MODE = False

# Имя INI файла настроек
SETTINGS_INI_FILENAME = './settings.ini'

# Кодировка командной оболочки по умолчанию
DEFAULT_ENCODING = sys.stdout.encoding if sys.platform.startswith('win') else locale.getpreferredencoding()

# Цвета в консоли
RED_COLOR_TEXT = '\x1b[31;1m'       # red
GREEN_COLOR_TEXT = '\x1b[32m'       # green
YELLOW_COLOR_TEXT = '\x1b[33;1m'    # yellow
BLUE_COLOR_TEXT = '\x1b[34m'        # blue
PURPLE_COLOR_TEXT = '\x1b[35m'      # purple
CYAN_COLOR_TEXT = '\x1b[36m'        # cyan
WHITE_COLOR_TEXT = '\x1b[37m'       # white
NORMAL_COLOR_TEXT = '\x1b[0m'       # normal


DEFAULT_DELETE_NOT_ACTUAL = False
DEFAULT_ACTUAL_PERIOD = '0000-01-00'

GET_1C_CLUSTERS_CMD_FMT = '%s cluster list %s:%s'
GET_1C_INFOBASES_CMD_FMT = '%s infobase --cluster=%s summary list %s:%s'
SET_1C_SCHEDULED_JOBS_CMD_FMT = '%s infobase --cluster=%s update --infobase=%s --infobase-user=%s --infobase-pwd=%s --scheduled-jobs-deny=%s %s:%s'
SET_1C_SESSIONS_DENY_CMD_FMT = '%s infobase --cluster=%s update --infobase=%s --infobase-user=%s --infobase-pwd=%s --sessions-deny=%s %s:%s'
GET_1C_SESSIONS_CMD_FMT = '%s session list --cluster=%s %s:%s'
GREP_SESSIONS_CMD_FMT = 'grep %s --before-context=2 --after-context=3 %s'
TERMINATE_1C_SESSION_CMD_FMT = '%s session --cluster=%s terminate --session=%s %s:%s'
IS_LOCKED_1C_INFOBASE_CMD_FMT = '%s lock --cluster=%s list --infobase=%s %s:%s'
DT_FILENAME_1C_FMT = '-%Y-%m-%d-%H-%M-%S.dt'
ADMIN_1C_NAME = u'Администратор'
ADMIN_1C_PASSWORD = '123123'
GET_1C_DT_FILE_FMT = '%s CONFIG /DumpIB %s /Out %s /S %s\\\\%s /N %s /P %s /DumpResult %s'
SYSTEM_TIME_SLEEP = 3
LOCK_TIME_SLEEP = 600

DEFAULT_ROOT_PASSWORD = '123456'

LINUX_NFS_MOUNT_CMD_FMT = 'echo "%s" | sudo --stdin mount --verbose --types nfs %s %s:/%s %s'
LINUX_NFS_UMOUNT_CMD_FMT = 'echo "%s" | sudo --stdin umount --verbose %s'

DEFAULT_NFS_AUTO_DELETE_DELAY = 1

# Отчет о создании резервной копии
BACKUP_REPORT_LINE_FMT = u'%s (%s) Файл <%s> - %s\n'
BACKUP_REPORT = ''
BACKUP_DATE_BLOCK = '{{ BACKUP_DATE }}'
REPORT_ENABLE = None
REPORT_FROM = None
REPORT_TO = None
REPORT_SUBJECT = ''
SMTP_SERVER = ''
SMTP_SERVER_PORT = 25
SMTP_LOGIN = None
SMTP_PASSWORD = None


def get_default_encoding():
    """
    Определить актуальную кодировку для вывода текста.

    :return: Актуальная кодировка для вывода текста.
    """
    return DEFAULT_ENCODING


def print_color_txt(txt, color=NORMAL_COLOR_TEXT):
    """
    Печать цветного текста.

    :param txt: Текст.
    :param color: Консольный цвет.
    """
    if sys.platform.startswith('win'):
        # Для Windows систем цветовая раскраска отключена
        txt = txt
    else:
        # Добавление цветовой раскраски
        txt = color + txt + NORMAL_COLOR_TEXT
    print(txt)


def debug(message=u''):
    """
    Вывести ОТЛАДОЧНУЮ информацию.

    :param message: Текстовое сообщение.
    """
    global DEBUG_MODE
    if DEBUG_MODE:
        print_color_txt('DEBUG. ' + message, BLUE_COLOR_TEXT)


def info(message=u''):
    """
    Вывести ТЕКСТОВУЮ информацию.

    :param message: Текстовое сообщение.
    """
    global DEBUG_MODE
    if DEBUG_MODE:
        print_color_txt(message, GREEN_COLOR_TEXT)


def error(message=u''):
    """
    Вывести информацию ОБ ОШИБКЕ.

    :param message: Текстовое сообщение.
    """
    global DEBUG_MODE
    if DEBUG_MODE:
        print_color_txt('ERROR. ' + message, RED_COLOR_TEXT)


def warning(message=u''):
    """
    Вывести информацию ОБ ПРЕДУПРЕЖДЕНИИ.

    :param message: Текстовое сообщение.
    """
    global DEBUG_MODE
    if DEBUG_MODE:
        print_color_txt('WARNING. ' + message, YELLOW_COLOR_TEXT)


def fatal(message=u''):
    """
    Вывести информацию ОБ ОШИБКЕ.

    :param message: Текстовое сообщение.
    """
    global DEBUG_MODE
    if not DEBUG_MODE:
        return

    trace_txt = traceback.format_exc()

    try:
        msg = message + u'\n' + trace_txt
    except UnicodeDecodeError:
        if not isinstance(message, str):
            message = str(message)
        if not isinstance(trace_txt, str):
            trace_txt = str(trace_txt)
        msg = message + u'\n' + trace_txt

    print_color_txt('FATAL. ' + msg, RED_COLOR_TEXT)


def main(*argv):
    """
    Главная запускаемая функция.

    :param argv: Параметры командной строки.
    :return:
    """
    global DEBUG_MODE
    global DIALOG_MODE
    global SETTINGS_INI_FILENAME

    global REPORT_ENABLE
    global REPORT_FROM
    global REPORT_TO
    global REPORT_SUBJECT
    global SMTP_SERVER
    global SMTP_SERVER_PORT
    global SMTP_LOGIN
    global SMTP_PASSWORD

    host = None
    port = None
    name = None
    path_1c = None
    backup = None
    delete = DEFAULT_DELETE_NOT_ACTUAL
    actual_period = DEFAULT_ACTUAL_PERIOD
    admin = None
    password = None
    scheduled_jobs = False

    try:
        options, args = getopt.getopt(argv, 'h?vd',
                                      ['help', 'version', 'debug',
                                       'dlg',
                                       'settings=',
                                       'host=', 'port=', 'name=', 'path_1c=', 'backup=',
                                       'delete', 'actual_period=',
                                       'admin=', 'password=',
                                       'scheduled_jobs',
                                       'report_enable',
                                       'report_from=', 'report_to=',
                                       'report_subject=',
                                       'smtp_server=', 'smtp_server_port=', 'smtp_login=', 'smtp_password='
                                       ])
    except getopt.error as msg:
        print_color_txt(str(msg), RED_COLOR_TEXT)
        print_color_txt(__doc__, GREEN_COLOR_TEXT)
        sys.exit(2)

    for option, arg in options:
        if option in ('-h', '--help', '-?'):
            print_color_txt(__doc__, GREEN_COLOR_TEXT)
            sys.exit(0)
        elif option in ('-v', '--version'):
            str_version = 'Версия: %s' % '.'.join([str(sign) for sign in __version__])
            print_color_txt(str_version, GREEN_COLOR_TEXT)
            sys.exit(0)
        elif option in ('-d', '--debug'):
            DEBUG_MODE = True
            info(u'Включен режим отладки')
        elif option == '--dlg':
            DIALOG_MODE = True
            info(u'Включен диалоговый режим работы программы')
        elif option == '--settings':
            SETTINGS_INI_FILENAME = arg
            info(u'Файл настроек <%s>' % SETTINGS_INI_FILENAME)

        elif option == '--host':
            host = arg
            info(u'\tHost: %s' % host)
        elif option == '--port':
            port = arg
            info(u'\tPort: %s' % port)
        elif option == '--name':
            name = arg
            info(u'\tName: %s' % name)
        elif option == '--path_1c':
            path_1c = arg
            info(u'\tPath 1C: %s' % path_1c)
        elif option == '--backup':
            backup = arg
            info(u'\tBackup to: %s' % backup)
        elif option == '--delete':
            delete = True
            info(u'\tDelete not actual')
        elif option == '--actual_period':
            actual_period = arg
            info(u'\tActual period: %s' % actual_period)
        elif option == '--admin':
            admin = arg
            info(u'\tAdministrator 1C: %s' % admin)
        elif option == '--password':
            password = arg
            info(u'\tAdministrator 1C password: %s' % password)
        elif option == '--scheduled_jobs':
            scheduled_jobs = True
            info(u'\tSet ON/OFF scheduled jobs mode')

        elif option == '--report_enable':
            REPORT_ENABLE = True
            info(u'\tReport enabled')
        elif option == '--report_from':
            REPORT_FROM = arg
            info(u'\tReport from: %s' % REPORT_FROM)
        elif option == '--report_to':
            if REPORT_TO is None:
                REPORT_TO = list()
            REPORT_TO.append(arg)
            info(u'\tReport to: %s' % str(REPORT_TO))
        elif option == '--report_subject':
            REPORT_SUBJECT = arg.replace(BACKUP_DATE_BLOCK, str(datetime.date.today()))
            info(u'\tReport subject: %s' % REPORT_SUBJECT)
        elif option == '--smtp_server':
            SMTP_SERVER = arg
            info(u'\tSMTP server: %s' % SMTP_SERVER)
        elif option == '--smtp_server_port':
            SMTP_SERVER_PORT = arg
            info(u'\tSMTP server port: %s' % SMTP_SERVER_PORT)
        elif option == '--smtp_login':
            SMTP_LOGIN = arg
            info(u'\tSMTP server login: %s' % SMTP_LOGIN)
        elif option == '--smtp_password':
            SMTP_PASSWORD = arg
            info(u'\tSMTP server password: %s' % SMTP_PASSWORD)

        else:
            msg = u'Не поддерживаемый параметр командной строки <%s>' % option
            warning(msg)

    try:
        if host and port and name and path_1c and backup and admin and password:
            info(u'Все параметры создания резервной копии информационной базы 1С заданы явно')
            backup_1c(host=host,
                      port=port,
                      name=name,
                      path_1c=path_1c,
                      backup=backup,
                      delete=delete,
                      actual_period=actual_period,
                      admin=admin,
                      password=password,
                      scheduled_jobs=scheduled_jobs)
        else:
            run(dlg_mode=DIALOG_MODE, settings_filename=SETTINGS_INI_FILENAME)

        # Если отправка отчета не включена, то проверяем может она включена в настройках
        if REPORT_ENABLE is None and os.path.exists(SETTINGS_INI_FILENAME):
            settings = ini2dict(ini_filename=SETTINGS_INI_FILENAME)
            REPORT_ENABLE = settings.get('SETTINGS', dict()).get('report_enable', False)
            REPORT_FROM = settings.get('SETTINGS', dict()).get('report_from', None)
            REPORT_TO = settings.get('SETTINGS', dict()).get('report_to', tuple())
            REPORT_SUBJECT = settings.get('SETTINGS', dict()).get('report_subject', '').replace(BACKUP_DATE_BLOCK, str(datetime.date.today()))
            SMTP_SERVER = settings.get('SETTINGS', dict()).get('smtp_server', None)
            SMTP_SERVER_PORT = settings.get('SETTINGS', dict()).get('smtp_server_port', 25)
            SMTP_LOGIN = settings.get('SETTINGS', dict()).get('smtp_login', None)
            SMTP_PASSWORD = settings.get('SETTINGS', dict()).get('smtp_password', None)

        # Если отправка отчета включена, то отправляем отчет
        if REPORT_ENABLE:
            global BACKUP_REPORT
            send_mail(send_from=REPORT_FROM, send_to=REPORT_TO,
                      subject=REPORT_SUBJECT, body=BACKUP_REPORT,
                      smtp_server=SMTP_SERVER, smtp_server_port=SMTP_SERVER_PORT,
                      login=SMTP_LOGIN, password=SMTP_PASSWORD)
    except:
        fatal(u'Ошибка выполнения:')


def run(dlg_mode=False, settings_filename=None):
    """
    Основная исполняемая процедура.

    :param dlg_mode: Включен диалоговый режим работы программы?
    :param settings_filename: Имя INI файла настроек.
        Если не определен, то берется по умолчанию.
    :return: True/False.
    """
    global SETTINGS_INI_FILENAME

    if settings_filename is None:
        settings_filename = SETTINGS_INI_FILENAME

    if not os.path.exists(settings_filename):
        error(u'Файл настроек <%s> не найден' % settings_filename)
        return False

    # Прочитали настройки
    settings = ini2dict(ini_filename=settings_filename)

    try:
        results = True
        if not dlg_mode:
            bases = settings.get('SETTINGS', dict()).get('bases', list())
            if bases:
                for base_name in bases:
                    info(u'Параметры создания резервной копии информационной базы 1С <%s> загружены их файла <%s>' % (base_name, settings_filename))
                    base = settings.get(base_name, dict())
                    host = base.get('host', None)
                    port = base.get('port', None)
                    name = base.get('name', None)
                    path_1c = base.get('path_1c', None)
                    backup = base.get('backup', None)
                    delete = base.get('delete', False)
                    actual_period = base.get('actual_period', None)
                    admin = base.get('admin', ADMIN_1C_NAME)
                    password = base.get('password', ADMIN_1C_PASSWORD)
                    scheduled_jobs = base.get('schduled_jobs', False)
                    description = base.get('description', '')
                    result = backup_1c(host=host,
                                       port=port,
                                       name=name,
                                       path_1c=path_1c,
                                       backup=backup,
                                       delete=delete,
                                       actual_period=actual_period,
                                       admin=admin,
                                       password=password,
                                       scheduled_jobs=scheduled_jobs,
                                       description=description)
                    results = results and result
            else:
                warning(u'Не определен список обрабатываемых баз 1С')
                results = False
        else:
            results = run_dialog_mode(settings=settings)
            if results:
                # По окончании обработки записать отредактированные данные в INI файл настроек
                dict2ini(src_dictionary=settings, ini_filename=settings_filename)

        return results
    except:
        fatal(u'Ошибка создания бэкапа 1С баз')

    return False


def run_dialog_mode(settings):
    """
    Запуск бэкапа в диалоговом режиме.

    :param settings: Словарь настроек.
    :return: True/False.
    """
    dlg = dialog.Dialog(dialog='dialog')
    title = u'Создание резервных копий информационных баз 1С'
    dlg.set_background_title(title)

    if not settings:
        dlg.msgbox(text=u'Не определены настройки обрабатываемых информационных баз 1С',
                   title=u'ВНИМАНИЕ!',
                   height=7,
                   width=120)
        return False

    selected_bases = list()
    while True:
        # Форма выбора баз из списка
        base_names = [section for section in settings.keys() if section != 'SETTINGS']
        bases = [settings.get(base_name, dict()) for base_name in base_names]
        choices = [(base.get('name', 'Unknown'),
                    base.get('description', ''),
                    False,
                    base.get('description', '')) for base in bases]

        code, tags = dlg.checklist(text=u'Информационные базы 1С:',
                                   choices=choices,
                                   title=u'Создание резервных копий',
                                   # height=15,
                                   width=120,
                                   list_height=7,
                                   help_button=True, item_help=True,
                                   help_tags=True, help_status=True)

        if code == dlg.OK:
            os.system('clear')
            selected_bases = [base for base in bases if base['name'] in tags]
            break
        elif code == dlg.CANCEL:
            os.system('clear')
            break
        elif code == dlg.HELP:
            description = [tag[3] for tag in tags[2] if tag[0] == tags[0]][0]
            dlg.msgbox(text=description,
                       title=u'Описание <%s>' % tags[0],
                       height=15,
                       width=120)

    result = False
    if selected_bases:
        for i, base in enumerate(selected_bases):
            while True:
                # Форма изменения параметров базы
                items = [(u'Наименование', 1, 1, str(base.get('name', '')), 1, 35, 60, 100, 0x0),
                         (u'Сервер', 2, 1, str(base.get('host', 'localhost')), 2, 35, 30, 30, 0x0),
                         (u'Порт', 3, 1, str(base.get('port', '1545')), 3, 35, 10, 10, 0x0),
                         (u'Описание', 4, 1, str(base.get('description', '')), 4, 35, 100, 255, 0x0),
                         (u'Путь к 1С', 5, 1, str(base.get('path_1c', '/opt/1cv8/x86_64/')), 5, 35, 100, 255, 0x0),
                         (u'Ресурс хранения резервных копий', 6, 1, str(base.get('backup', 'nfs://server:/share/folder')), 6, 35, 100, 255, 0x0),
                         (u'Администратор 1С', 7, 1, str(base.get('admin', ADMIN_1C_NAME)), 7, 35, 100, 255, 0x0),
                         (u'Пароль администратора 1С', 8, 1, str(base.get('password', ADMIN_1C_PASSWORD)), 8, 35, 100, 255, 0x0),
                         #(u'Режим вкл/выкл регламентных заданий', 8, 1, str(base.get('scheduled_jobs', False)), 9, 35, 100, 255, 0x0),
                         ]
                text = base.get('description', u'Информационная база 1С')
                code, tags = dlg.mixedform(text,
                                           items,
                                           # height=14,
                                           width=160,
                                           title=u'Информационная база 1С',
                                           help_button=True, help_status=True)

                if code == dlg.OK:
                    os.system('clear')
                    # Сохраняем отредактированные значения в настройках
                    name, host, port, description, path_1c, backup, admin, password = tags
                    selected_bases[i]['name'] = name
                    selected_bases[i]['host'] = host
                    selected_bases[i]['port'] = port
                    selected_bases[i]['description'] = description
                    selected_bases[i]['path_1c'] = path_1c
                    selected_bases[i]['backup'] = backup
                    selected_bases[i]['admin'] = admin
                    selected_bases[i]['password'] = password

                    # Проверяем отредактированные значения
                    rac_filename = os.path.join(path_1c, 'rac')
                    if not os.path.exists(rac_filename):
                        dlg.msgbox(text=u'Не найден файл утилиты rac <%s>' % rac_filename,
                                   title=u'ВНИМАНИЕ!',
                                   height=15,
                                   width=120)
                        continue

                    break
                else:
                    # 't' contains the list of items as filled by the user
                    break
    else:
        dlg.msgbox(text=u'Информационные базы 1C не выбраны для создания резервных копий',
                   title=u'ВНИМАНИЕ!',
                   height=15,
                   width=120)
        os.system('clear')

    if selected_bases:
        result = True
        info(u'Запуск создания резервных копий информацинных баз 1с %s' % str([base['name'] for base in selected_bases]))
        for base in selected_bases:
            host = base.get('host', None)
            port = base.get('port', None)
            name = base.get('name', None)
            path_1c = base.get('path_1c', None)
            backup = base.get('backup', None)
            delete = base.get('delete', False)
            actual_period = base.get('actual_period', None)
            admin = base.get('admin', ADMIN_1C_NAME)
            password = base.get('password', ADMIN_1C_PASSWORD)
            scheduled_jobs = base.get('scheduled_jobs', False)
            description = base.get('description', '')
            result = result and backup_1c(host=host,
                                          port=port,
                                          name=name,
                                          path_1c=path_1c,
                                          backup=backup,
                                          delete=delete,
                                          actual_period=actual_period,
                                          admin=admin,
                                          password=password,
                                          scheduled_jobs=scheduled_jobs,
                                          description=description)
    else:
        warning(u'Не выбраны информационные базы 1C для создания резервных копий')

    return result


def ini2dict(ini_filename, encoding=DEFAULT_ENCODING):
    """
    Загрузить INI файл как словарь.

    :param ini_filename: Полное имя INI файла настроек.
    :return: Словарь настроек или None в случае ошибки.
    """
    ini_file = None
    try:
        if not os.path.exists(ini_filename):
            warning(u'INI файл <%s> не найден' % ini_filename)
            return None

        ini_parser = configparser.ConfigParser()
        ini_file = open(ini_filename, 'rt', encoding=encoding)
        ini_parser.read_file(ini_file)
        ini_file.close()

        ini_dict = {}
        sections = ini_parser.sections()
        for section in sections:
            params = ini_parser.options(section)
            ini_dict[section] = {}
            for param in params:
                param_str = ini_parser.get(section, param)
                try:
                    # Perhaps in the form of a parameter is recorded a dictionary / list / None / number, etc.
                    param_value = eval(param_str)
                except:
                    # No, it's a string.
                    param_value = param_str

                debug(u'\t%s.%s = %s' % (section, param, param_value))

                ini_dict[section][param] = param_value

        return ini_dict
    except:
        if ini_file:
            ini_file.close()
        fatal(u'Error converting INI file <%s> to dictionary' % ini_filename)
    return None


def dict2ini(src_dictionary, ini_filename, rewrite=False, encoding=DEFAULT_ENCODING):
    """
    Записать словарь в INI файл.

    :param src_dictionary: Исходный словарь.
    :param ini_filename: Полное имя INI файла настроек.
    :param rewrite: Перезаписать существующий INI файл?
    :return: True/False.
    """
    ini_file = None
    try:
        if not src_dictionary:
            warning(u'No dictionary defined for saving to INI file. <%s>' % src_dictionary)
            return False

        ini_file_name = os.path.split(ini_filename)
        path = ini_file_name[0]
        if not os.path.isdir(path):
            os.makedirs(path)

        if not os.path.exists(ini_filename) or rewrite:
            ini_file = open(ini_filename, 'wt', encoding=encoding)
            ini_file.write('')
            ini_file.close()

        ini_parser = configparser.ConfigParser()
        ini_file = open(ini_filename, 'rt', encoding=encoding)
        ini_parser.read_file(ini_file)
        ini_file.close()

        for section in src_dictionary.keys():
            section_str = str(section)
            if not ini_parser.has_section(section_str):
                ini_parser.add_section(section_str)

            for param in src_dictionary[section].keys():
                param_name = str(param)
                param_value = str(src_dictionary[section][param])
                debug(u'Установка значения в INI файл <%s> [%s].%s = %s' % (ini_filename, section_str, param_name, param_value))
                ini_parser.set(section_str, param_name, param_value)

        ini_file = open(ini_filename, 'wt', encoding=encoding)
        ini_parser.write(ini_file)
        ini_file.close()

        return True
    except:
        if ini_file:
            ini_file.close()
        fatal(u'Error saving dictionary in INI file <%s>' % ini_filename)
    return False


def save_text_file(txt_filename, txt='', rewrite=True, encoding=DEFAULT_ENCODING):
    """
    Save text file.

    :param txt_filename: Text file name.
    :param txt: Body text file as unicode.
    :param rewrite: Rewrite file if it exists?
    :param encoding: Text file code page.
    :return: True/False.
    """
    if not isinstance(txt, str):
        txt = str(txt)

    file_obj = None
    try:
        if rewrite and os.path.exists(txt_filename):
            os.remove(txt_filename)
            info(u'Remove file <%s>' % txt_filename)
        if not rewrite and os.path.exists(txt_filename):
            warning(u'File <%s> not saved' % txt_filename)
            return False

        # Check path
        txt_dirname = os.path.dirname(txt_filename)
        if not os.path.exists(txt_dirname):
            os.makedirs(txt_dirname)

        file_obj = open(txt_filename, 'wt', encoding=encoding)
        file_obj.write(txt)
        file_obj.close()
        info(u'Текстовый файл <%s> сохранен' % txt_filename)
        return True
    except:
        if file_obj:
            file_obj.close()
        fatal('Save text file <%s> error' % txt_filename)
    return False


def get_lines_exec_cmd(cmd):
    """
    Получить строки - результат выполнения команды системы.

    :param cmd: Комманда ОС.
    :return: Список строк или пустой список в случае ошибки.
    """
    lines = list()
    try:
        info(u'Выполнение команды <%s>' % cmd)

        cmd_list = cmd.split(' ')
        process = subprocess.Popen(cmd_list, stdout=subprocess.PIPE)
        b_lines = process.stdout.readlines()
        console_encoding = locale.getpreferredencoding()
        lines = [line.decode(console_encoding).strip() for line in b_lines]
    except:
        fatal(u'Ошибка получения результатов выполнения команды ОС <%s>' % cmd)
    return lines


def backup_1c(host=None, port=None, name=None, path_1c=None, backup=None, delete=None, actual_period=None,
              admin=ADMIN_1C_NAME, password=ADMIN_1C_PASSWORD, scheduled_jobs=False, sessions_deny=True,
              description=''):
    """
    Выполнить бэкап базы 1С.

    :param host: Сервер 1С
    :param port: Порт утилиты ras
        На сервере приложений 1С должна быть запущена утилита ras в режиме демона:
        /opt/1cv8/x86_64/ras --daemon cluster --port=1545
    :param name: Наименование информационной базы 1C
    :param path_1c: Путь к установленным программам 1С
        Например: /opt/1cv8/x86_64
    :param backup: Указание ресурса для хранения бэкапов
        Например: nfs://workgroup;user:password@server:/share/folder
    :param delete: Удалить не актуальные бэкапы?
    :param actual_period: Временной период актуальности бэкапов
        Например: 0000-00-10 (10 дней)
    :param admin: Администратор 1С.
    :param password: Пароль администратора 1С.
    :param scheduled_jobs: Режим вкл/откл регламентных заданий в процессе создания бэкапов.
    :param sessions_deny:  Режим вкл/откл блокировка начала сеансов.
    :param description: Описание информационной базы 1С для отчета.
    :return: True/False.
    """
    start_time = time.time()
    info(u'Запуск создания резервной копии базы 1С <%s>' % name)

    result = False
    dt_filename = ''

    rac_filename = os.path.join(path_1c, 'rac')
    # 1. Удаляем сеансы
    # 1.1. Получить список кластеров
    cmd = GET_1C_CLUSTERS_CMD_FMT % (rac_filename, host, port)
    cluster_lines = get_lines_exec_cmd(cmd)
    for i_cluster in range(int(len(cluster_lines) / 15)):
        cluster_id = cluster_lines[i_cluster * 15].split(':')[1].strip()
        # 1.2. Выбираем информационную базу 1С по имени
        cmd = GET_1C_INFOBASES_CMD_FMT % (rac_filename, cluster_id, host, port)
        infobase_lines = get_lines_exec_cmd(cmd)
        for i_infobase in range(int(len(infobase_lines) / 4)):
            # 1.3. Получаем идентификатор информационной базы 1С
            infobase_id = infobase_lines[i_infobase * 4].split(':')[1].strip()
            infobase_name = infobase_lines[i_infobase * 4 + 1].split(':')[1].strip()
            # Проверку по имени баз 1с делаем регистронечувствительной
            if infobase_name.lower() == name.strip().lower():
                # 1.4. Получаем список открытых сеансов данной информационной базы 1С
                sessions_filename = tempfile.mktemp()
                cmd = GET_1C_SESSIONS_CMD_FMT % (rac_filename, cluster_id, host, port)
                session_lines = get_lines_exec_cmd(cmd)
                save_text_file(txt_filename=sessions_filename, txt=os.linesep.join(session_lines))
                if not os.path.exists(sessions_filename):
                    warning(u'Промежуточный файл сеансов 1С <%s> не сформирован' % sessions_filename)
                    continue

                if scheduled_jobs:
                    # ВНИМАНИЕ! Включаем режим блокировки регламентных заданий информационной базы 1с
                    cmd = SET_1C_SCHEDULED_JOBS_CMD_FMT % (rac_filename, cluster_id, infobase_id, admin, password, 'on', host, port)
                    set_scheduled_jobs_lines = get_lines_exec_cmd(cmd)
                    if set_scheduled_jobs_lines:
                        for line in set_scheduled_jobs_lines:
                            warning(line.strip())

                if sessions_deny:
                    # ВНИМАНИЕ! Включаем режим блокировки начала сеансов
                    cmd = SET_1C_SESSIONS_DENY_CMD_FMT % (rac_filename, cluster_id, infobase_id, admin, password, 'on', host, port)
                    set_sessions_deny_lines = get_lines_exec_cmd(cmd)
                    if set_sessions_deny_lines:
                        for line in set_sessions_deny_lines:
                            warning(line.strip())

                cmd = GREP_SESSIONS_CMD_FMT % (infobase_id, sessions_filename)
                session_lines = get_lines_exec_cmd(cmd)
                session_count = (int(len(session_lines) / 7) + 1) if 0 < len(session_lines) else 0
                info(u'Открытых сеансов [%d]' % session_count)
                if os.path.exists(sessions_filename):
                    os.remove(sessions_filename)
                    info(u'Текстовый файл сеансов 1С <%s> удален' % sessions_filename)
                for i_session in range(session_count):
                    # 1.5. Перебираем сеансы и убиваем их
                    session_id = session_lines[i_session * 7].split(':')[1].strip()
                    # session_id = session_lines[i_session * 7 + 1].split(':')[1].strip()
                    username = session_lines[i_session * 7 + 5].split(':')[1].strip()
                    cmd = TERMINATE_1C_SESSION_CMD_FMT % (rac_filename, cluster_id, session_id, host, port)
                    info(u'Закрытие сеанса <%s>' % cmd)
                    os.system(cmd)
                    info(u'Закрыт сеанс [%s] пользователя <%s>' % (session_id, username))

                # Задержка после выполнения команды
                if LOCK_TIME_SLEEP:
                    time.sleep(LOCK_TIME_SLEEP)

                if sessions_deny:
                    # ВНИМАНИЕ! Выключаем режим блокировки начала сеансов
                    cmd = SET_1C_SESSIONS_DENY_CMD_FMT % (rac_filename, cluster_id, infobase_id, admin, password, 'off', host, port)
                    set_sessions_deny_lines = get_lines_exec_cmd(cmd)
                    if set_sessions_deny_lines:
                        for line in set_sessions_deny_lines:
                            warning(line.strip())

                # Сгененировать имена импользуемых файлов
                dt_filename = os.path.join(tempfile.gettempdir(),
                                           name + datetime.datetime.now().strftime(DT_FILENAME_1C_FMT))
                out_log_filename = '%s.log' % tempfile.mktemp()
                result_log_filename = '%s.log' % tempfile.mktemp()

                # Проверка на блокировки информационной базы 1С
                # Если блокировки есть, то просто выдаем предупреждение и пропускаем обработку
                cmd = IS_LOCKED_1C_INFOBASE_CMD_FMT % (rac_filename, cluster_id, infobase_id, host, port)
                lock_lines = get_lines_exec_cmd(cmd)
                if lock_lines:
                    warning(u'Блокировки информационной базы 1С <%s>:' % infobase_name)
                    for line in lock_lines:
                        warning('\t%s' % line)
                    # warning(u'Информационная база 1С <%s> заблокирована. Создание резервной копии не возможно' % infobase_name)
                    warning(u'Информационная база 1С <%s> заблокирована. Попытка создания резервной копии' % infobase_name)
                if True:
                    # 2. Запуск команды получения резервной копии
                    prg_1cv8_filename = os.path.join(path_1c, '1cv8')
                    cmd = GET_1C_DT_FILE_FMT % (prg_1cv8_filename,
                                                dt_filename,
                                                out_log_filename,
                                                host,
                                                name,
                                                admin,
                                                password,
                                                result_log_filename)
                    info(u'Выполнение команды <%s>' % cmd)
                    os.system(cmd)

                    # Задержка после выполнения команды
                    if SYSTEM_TIME_SLEEP:
                        time.sleep(SYSTEM_TIME_SLEEP)

                if scheduled_jobs:
                    # ВНИМАНИЕ! Выключаем режим блокировки регламентных заданий информационной базы 1с
                    cmd = SET_1C_SCHEDULED_JOBS_CMD_FMT % (rac_filename, cluster_id, infobase_id, admin, password, 'off', host, port)
                    set_scheduled_jobs_lines = get_lines_exec_cmd(cmd)
                    if set_scheduled_jobs_lines:
                        for line in set_scheduled_jobs_lines:
                            warning(line.strip())

                # 3. Выводим на экран журнал выгрузки
                if os.path.exists(out_log_filename):
                    info(u'Журнал выполнения:')
                    os.system('cat %s' % out_log_filename)
                    os.remove(out_log_filename)
                    info(u'Удален файл журнала <%s>' % out_log_filename)
                if os.path.exists(result_log_filename):
                    info(u'Результат:')
                    os.system('cat %s' % result_log_filename)
                    os.remove(result_log_filename)
                    info(u'Удален файл журнала <%s>' % result_log_filename)

                # 4. Копирование файла на сервер бекапов
                if os.path.exists(dt_filename):
                    result = upload_nfs_file(upload_url=backup, filename=dt_filename)
                    if result:
                        if os.path.exists(dt_filename):
                            info(u'Удаление локальной резервной копии информационной базы 1С <%s>' % dt_filename)
                            os.remove(dt_filename)
                else:
                    error(u'Файл <%s> резервной копии информационной базы 1с <%s> не создан' % (dt_filename, name))

                # 5. Если необходимо, то удаляем не актуальные резервные копии

                # Окончание создания резервной копии и выход
                break

    info(u'Останов создания резервной копии базы 1С <%s> ... %s' % (name, time.time() - start_time))

    # Заполняем отчет
    global BACKUP_REPORT
    BACKUP_REPORT += BACKUP_REPORT_LINE_FMT % (name, description, os.path.basename(dt_filename), u'Да' if result else u'НЕТ')

    return result


def split_nfs_url_path(url):
    """
    Correct breakdown of the NFS resource URL path into components.
    If the <#> character occurs in the path, the URL parsing library perceives further
    standing characters as fragment. This should be taken into account.
    This is what this function is designed for.

    :param url: urlparse.ParseResult object.
    :return: List of elements of the path to the SMB resource.
    """
    path_list = url.path.split(os.path.sep)
    if url.fragment:
        fragment_path_list = url.fragment.split(os.path.sep)
        fragment_path_list[0] = u'#' + fragment_path_list[0]
        path_list += fragment_path_list
    return path_list


def get_nfs_path_from_url(url):
    """
    Determine the path to the NFS resource by URL.

    :param url: NFS resource URL.
    :return: Samba resource path.
    """
    url = urllib.parse.urlparse(url)
    path_list = split_nfs_url_path(url)
    smb_path = os.path.join(*path_list)
    return smb_path


def get_nfs_host_from_url(url):
    """
    Determine NFS resource host by URL.

    :param url: NFS resource URL.
    :return: Samba resource path.
    """
    url = urllib.parse.urlparse(url)
    host = url.netloc.split('@')[1] if '@' in url.netloc else url.netloc
    return host.replace(':', '')


def valid_ping_host(host_name):
    """
    Check connect with host by ping.

    :param host_name: Host name/ip address.
    :return: True - connected. False - not connected.
    """
    if sys.platform.startswith('win'):
        response = os.system('ping -n 1 %s' % host_name)
    elif sys.platform.startswith('linux'):
        response = os.system('ping -c 1 %s' % host_name)
    else:
        return False
    return response == 0


def mount_nfs_resource(url, dst_path=None, options=None, root_password=None):
    """
    Смонтировать NFS ресурс.

    :param url: Nfs resource URL.
    :param dst_path: Destination path. If not defined then create template destination path.
    :param options: Additional mount options.
    :param root_password: Root user password.
    :return: True/False.
    """
    if dst_path is None:
        dst_path = tempfile.mktemp()
        if not os.path.exists(dst_path):
            os.makedirs(dst_path)

    if isinstance(options, str):
        options = '--options %s' % options.replace(' ', '')
    elif isinstance(options, (tuple, list)):
        options = '--options %s' % ','.join([str(item) for item in options])
    elif isinstance(options, dict):
        options = '--options %s' % ','.join(['%s=%s' % (str(opt_name), str(opt_value)) for opt_name, opt_value in options.items()])
    else:
        options = ''

    if root_password is None:
        root_password = DEFAULT_ROOT_PASSWORD

    try:
        nfs_host = get_nfs_host_from_url(url)
        if not valid_ping_host(nfs_host):
            warning(u'NFS resource host <%s> not found' % nfs_host)
            return False

        nfs_path = get_nfs_path_from_url(url)
        mount_cmd = LINUX_NFS_MOUNT_CMD_FMT % (root_password, options, nfs_host, nfs_path, dst_path)
        os.system(mount_cmd)
        info(u'NFS resource <%s> mounted to <%s>' % (url, dst_path))
        return True
    except:
        fatal(u'Error mount NFS resource <%s>' % url)
    return False


def umount_nfs_resource(mnt_path, root_password=None, auto_delete=False):
    """
    Linux. Umount NFS resource.

    :param mnt_path: Mount resource path.
    :param root_password: Root user password.
    :param auto_delete: Auto delete mount path after umount.
    :return: True/False.
    """
    if not mnt_path or not os.path.exists(mnt_path):
        warning(u'NFS resource mount folder <%s> not found' % mnt_path)
        return False

    if root_password is None:
        root_password = DEFAULT_ROOT_PASSWORD

    try:
        umount_cmd = LINUX_NFS_UMOUNT_CMD_FMT % (root_password, mnt_path)
        os.system(umount_cmd)
        if auto_delete:
            # Make a delay 1 sec
            if DEFAULT_NFS_AUTO_DELETE_DELAY:
                time.sleep(DEFAULT_NFS_AUTO_DELETE_DELAY)

            if not bool(os.listdir(mnt_path)):
                os.rmdir(mnt_path)
            else:
                warning(u'The mounted folder was not deleted because it is not empty')
                return False

        info(u'NFS resource umounted from <%s>' % mnt_path)
        return True
    except:
        fatal(u'Error umount NFS resource <%s>' % mnt_path)
    return False


def upload_nfs_file(upload_url=None, filename=None, dst_path=None, rewrite=True, mnt_path=None, *args, **kwargs):
    """
    Upload file to NFS resource.

    :param upload_url: NFS resource URL.
        For example:
        'nfs://SAFE/Backup'
    :param filename: Source file name.
        For example:
        '/home/user/2017/FDOC/RC001.DCM'
    :param dst_path: NFS resource path to save the file.
    :param rewrite: Overwrite a file if it already exists?
    :param mnt_path: Mount path in the case of an already NFS resource is mounted.
    :return: True/False.
    """
    result = False

    if not os.path.exists(filename):
        warning(u'Source file <%s> for upload to NFS resource not found' % filename)
        return result

    if mnt_path is None:
        mnt_path = tempfile.mktemp()
        if not os.path.exists(mnt_path):
            os.makedirs(mnt_path)
        mount_result = mount_nfs_resource(url=upload_url, dst_path=mnt_path, *args, **kwargs)
        mounted = True
    else:
        mount_result = True
        mounted = False

    if mount_result:
        if os.path.exists(mnt_path):
            mnt_res_path = mnt_path
            if dst_path:
                dst_filename = os.path.join(mnt_res_path, dst_path, os.path.basename(filename))
            else:
                dst_filename = os.path.join(mnt_res_path, os.path.basename(filename))
            if rewrite and os.path.exists(dst_filename):
                if os.path.exists(dst_filename):
                    info(u'Удаление файла <%s>' % dst_filename)
                    os.remove(dst_filename)
            info(u'Копирование файла <%s> -> <%s>' % (filename, dst_filename))
            result = shutil.copyfile(filename, dst_filename)
        else:
            warning(u'NFS resource mount path <%s> not found' % mnt_path)

    if mounted:
        umount_nfs_resource(mnt_path=mnt_path, auto_delete=True, *args, **kwargs)
    return result


def send_mail(send_from=None, send_to=(),
              subject=None, body=None, attached=(),
              smtp_server=None, smtp_server_port=None,
              login=None, password=None):
    """
    Функция отправки письма.

    :param send_from: Адрес с которого отсылается письмо.
    :param send_to: Список адресов на которые отсылается письмо.
    :param subject: Тема письма.
    :param body: Тело письма.
    :param attached: Список прикрепляемых файлов.
    :param smtp_server: SMTP сервер.
    :param smtp_server_port: Порт SMTP сервера, обычно 25.
    :param login: Логин на SMTP сервере.
    :param password: Пароль на SMTP сервере.
    :return: True/False.
    """
    # Проверка типов входных аргументов
    assert isinstance(send_to, (list, tuple))
    assert isinstance(attached, (list, tuple))
    assert isinstance(body, str)

    # Создать сообщение
    msg = email.mime.multipart.MIMEMultipart()

    msg['From'] = str(send_from) if send_from else ''
    msg['To'] = ', '.join(send_to) if send_to else ''
    msg['Date'] = email.utils.formatdate(localtime=True)
    msg['Subject'] = str(subject) if subject else ''
    msg.attach(email.mime.text.MIMEText(body))

    # Прикрепление файлов
    for filename in attached:
        part = email.mime.base.MIMEBase('application', 'octet-stream')
        part.set_payload(open(filename, 'rb').read())
        email.encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment; filename="%s"' % os.path.basename(filename))
        msg.attach(part)

        file_size = os.stat(filename).st_size
        info(u'File <%s> (%s) attached to email' % (filename, file_size))

    msg_txt = msg.as_string()

    # Соединение с SMTP сервером и отправка сообщения
    try:
        smtp = smtplib.SMTP(smtp_server, smtp_server_port)
        smtp.set_debuglevel(0)
        if login:
            smtp.login(login, password)

        smtp.sendmail(send_from, send_to, msg_txt)
        smtp.close()

        info(u'Email from <%s> to %s sended' % (send_from, send_to))

        return True
    except smtplib.SMTPException:
        fatal(u'Error send email')
    return False


if __name__ == '__main__':
    main(*sys.argv[1:])
