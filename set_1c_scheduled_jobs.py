#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт автоматического ВКЛ/ОТКЛ запуска регламентных заданий информационных баз 1С под Linux.

Необходимые пакеты:
sudo apt install nfs-common
sudo apt install python3-dialog

Дополнительная информация:
Запускается только на компьютере с установленным клиентом 1С.
На сервере приложений 1С должна быть запущена утилита ras в режиме демона:
/opt/1cv8/x86_64/ras --daemon cluster --port=1545

ВАЖНО:
Для запуска графических оконных приложений в cron необходимо указать export DISPLAY=:0
Например:
# m h  dom mon dow   command
0 10 * * * export DISPLAY=:0; python3 /home/user/prg/backup_1c_base/backup_1c_base.py --debug --settings=/home/user/prg/backup_1c_base/settings.ini 1>/home/user/prg/backup_1c_base/stdout.log 2>/home/user/prg/backup_1c_base/error.log

Запуск:

        python3 set_1c_scheduled_jobs.py [параметры командной строки]

Пример запуска:
        python3 set_1c_scheduled_jobs.py --debug --on

Параметры командной строки:

    [Помощь и отладка]
        --help|-h|-?        Помощь
        --version|-v        Версия программы
        --debug|-d          Включить сообщения отладки

    [Основные опции]
        --settings=         Явное указание файла настроек.
                            Если не указывается, то берется по умолчанию файл settings.ini.
        --host=             Сервер 1С
        --port=             Порт утилиты ras
                            На сервере приложений 1С должна быть запущена утилита ras в режиме демона:
                            /opt/1cv8/x86_64/ras --daemon cluster --port=1545
        --name=             Наименование базы
        --path_1c=          Путь к установленным программам 1С
                            Например: /opt/1cv8/x86_64
        --admin=            Администратор 1С
        --password=         Пароль администратора 1С
        --on                Вкл. блокировку запуска регламентных заданий информационной базы 1С
        --off               Откл. блокировку запуска регламентных заданий информационной базы 1С
"""

import sys
import getopt
import locale
import os
import os.path
import traceback
import subprocess
import time

try:
    import configparser
except ImportError:
    print('Import error configparser')

try:
    import dialog
except ImportError:
    print('Import error pythondialog. Install: sudo apt install python3-dialog')

__version__ = (0, 0, 1, 1)

# Режим отладки
DEBUG_MODE = False

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


GET_1C_CLUSTERS_CMD_FMT = '%s cluster list %s:%s'
GET_1C_INFOBASES_CMD_FMT = '%s infobase --cluster=%s summary list %s:%s'
SET_1C_SCHEDULED_JOBS_CMD_FMT = '%s infobase --cluster=%s update --infobase=%s --infobase-user=%s --infobase-pwd=%s --scheduled-jobs-deny=%s %s:%s'
DT_FILENAME_1C_FMT = '-%Y-%m-%d-%H-%M-%S.dt'
ADMIN_1C_NAME = u'Администратор'
ADMIN_1C_PASSWORD = '123123'
SYSTEM_TIME_SLEEP = 3

DEFAULT_ROOT_PASSWORD = '123456'


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
    print_color_txt('DEBUG. ' + message, BLUE_COLOR_TEXT)


def info(message=u''):
    """
    Вывести ТЕКСТОВУЮ информацию.

    :param message: Текстовое сообщение.
    """
    print_color_txt(message, GREEN_COLOR_TEXT)


def error(message=u''):
    """
    Вывести информацию ОБ ОШИБКЕ.

    :param message: Текстовое сообщение.
    """
    print_color_txt('ERROR. ' + message, RED_COLOR_TEXT)


def warning(message=u''):
    """
    Вывести информацию ОБ ПРЕДУПРЕЖДЕНИИ.

    :param message: Текстовое сообщение.
    """
    print_color_txt('WARNING. ' + message, YELLOW_COLOR_TEXT)


def fatal(message=u''):
    """
    Вывести информацию ОБ ОШИБКЕ.

    :param message: Текстовое сообщение.
    """
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
    global SETTINGS_INI_FILENAME

    host = None
    port = None
    name = None
    path_1c = None
    admin = None
    password = None
    on_or_off = False

    try:
        options, args = getopt.getopt(argv, 'h?vd',
                                      ['help', 'version', 'debug',
                                       'settings=',
                                       'host=', 'port=', 'name=', 'path_1c=',
                                       'admin=', 'password=',
                                       'on', 'off',
                                       ])
    except getopt.error as msg:
        error(str(msg))
        info(__doc__)
        sys.exit(2)

    for option, arg in options:
        if option in ('-h', '--help', '-?'):
            info(__doc__)
            sys.exit(0)
        elif option in ('-v', '--version'):
            str_version = 'Версия: %s' % '.'.join([str(sign) for sign in __version__])
            info(str_version)
            sys.exit(0)
        elif option in ('-d', '--debug'):
            DEBUG_MODE = True
            info(u'Включен режим отладки')
        elif option == '--settings':
            SETTINGS_INI_FILENAME = arg
            if DEBUG_MODE:
                info(u'Файл настроек <%s>' % SETTINGS_INI_FILENAME)

        elif option == '--host':
            host = arg
            if DEBUG_MODE:
                info(u'\tHost: %s' % host)
        elif option == '--port':
            port = arg
            if DEBUG_MODE:
                info(u'\tPort: %s' % port)
        elif option == '--name':
            name = arg
            if DEBUG_MODE:
                info(u'\tName: %s' % name)
        elif option == '--path_1c':
            path_1c = arg
            if DEBUG_MODE:
                info(u'\tPath 1C: %s' % path_1c)
        elif option == '--admin':
            admin = arg
            if DEBUG_MODE:
                info(u'\tAdministrator 1C: %s' % admin)
        elif option == '--password':
            password = arg
            if DEBUG_MODE:
                info(u'\tAdministrator 1C password: %s' % password)
        elif option == '--on':
            on_or_off = True
            info(u'Включена блокировка запуска регламентных заданий')
        elif option == '--off':
            on_or_off = False
            info(u'Отключена блокировка запуска регламентных заданий')

        else:
            if DEBUG_MODE:
                msg = u'Не поддерживаемый параметр командной строки <%s>' % option
                warning(msg)

    try:
        if host and port and name and path_1c and admin and password:
            if DEBUG_MODE:
                info(u'Все параметры вкл/откл запуска регламентных заданий информационной базы 1С заданы явно')
            set_scheduled_jobs_1c_infobase(host=host,
                                           port=port,
                                           name=name,
                                           path_1c=path_1c,
                                           admin=admin,
                                           password=password,
                                           on_or_off=on_or_off)
        else:
            run(settings_filename=SETTINGS_INI_FILENAME, on_or_off=on_or_off)
    except:
        if DEBUG_MODE:
            fatal(u'Ошибка выполнения:')


def run(settings_filename=None, on_or_off=False):
    """
    Основная исполняемая процедура.

    :param settings_filename: Имя INI файла настроек.
        Если не определен, то берется по умолчанию.
    :param on_or_off: True - вкл блокировка регламентных заданий, False - откл блокировки регламентных заданий.
    :return: True/False.
    """
    global DEBUG_MODE
    global SETTINGS_INI_FILENAME

    if settings_filename is None:
        settings_filename = SETTINGS_INI_FILENAME

    if not os.path.exists(settings_filename):
        if DEBUG_MODE:
            error(u'Файл настроек <%s> не найден' % settings_filename)
        return False

    # Прочитали настройки
    settings = ini2dict(ini_filename=settings_filename)

    try:
        results = True
        bases = settings.get('SETTINGS', dict()).get('bases', list())
        if bases:
            for base_name in bases:
                if DEBUG_MODE:
                    info(u'Параметры вкл/откл регламентных заданий информационной базы 1С <%s> загружены их файла <%s>' % (base_name, settings_filename))
                base = settings.get(base_name, dict())
                host = base.get('host', None)
                port = base.get('port', None)
                name = base.get('name', None)
                path_1c = base.get('path_1c', None)
                admin = base.get('admin', ADMIN_1C_NAME)
                password = base.get('password', ADMIN_1C_PASSWORD)
                result = set_scheduled_jobs_1c_infobase(host=host,
                                                        port=port,
                                                        name=name,
                                                        path_1c=path_1c,
                                                        admin=admin,
                                                        password=password,
                                                        on_or_off=on_or_off)
                results = results and result
        else:
            warning(u'Не определен список обрабатываемых баз 1С')
            results = False

        return results
    except:
        fatal(u'Ошибка вкл/откл регламентных заданий информационных баз 1С')

    return False


def ini2dict(ini_filename, encoding=DEFAULT_ENCODING):
    """
    Загрузить INI файл как словарь.

    :param ini_filename: Полное имя INI файла настроек.
    :return: Словарь настроек или None в случае ошибки.
    """
    global DEBUG_MODE

    ini_file = None
    try:
        if not os.path.exists(ini_filename):
            if DEBUG_MODE:
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

                if DEBUG_MODE:
                    debug(u'\t%s.%s = %s' % (section, param, param_value))

                ini_dict[section][param] = param_value

        return ini_dict
    except:
        if ini_file:
            ini_file.close()
        if DEBUG_MODE:
            fatal(u'Error converting INI file <%s> to dictionary' % ini_filename)
    return None


def get_lines_exec_cmd(cmd):
    """
    Получить строки - результат выполнения команды системы.

    :param cmd: Комманда ОС.
    :return: Список строк или пустой список в случае ошибки.
    """
    global DEBUG_MODE

    lines = list()
    try:
        if DEBUG_MODE:
            info(u'Выполнение команды <%s>' % cmd)

        cmd_list = cmd.split(' ')
        process = subprocess.Popen(cmd_list, stdout=subprocess.PIPE)
        b_lines = process.stdout.readlines()
        console_encoding = locale.getpreferredencoding()
        lines = [line.decode(console_encoding).strip() for line in b_lines]
    except:
        if DEBUG_MODE:
            fatal(u'Ошибка получения результатов выполнения команды ОС <%s>' % cmd)
    return lines


def set_scheduled_jobs_1c_infobase(host=None, port=None, name=None, path_1c=None,
                                   admin=ADMIN_1C_NAME, password=ADMIN_1C_PASSWORD,
                                   on_or_off=False):
    """
    Вкл/откл регламентных заданий информационной базы 1С.

    :param host: Сервер 1С
    :param port: Порт утилиты ras
        На сервере приложений 1С должна быть запущена утилита ras в режиме демона:
        /opt/1cv8/x86_64/ras --daemon cluster --port=1545
    :param name: Наименование информационной базы 1C
    :param path_1c: Путь к установленным программам 1С
        Например: /opt/1cv8/x86_64
    :param admin: Администратор 1С.
    :param password: Пароль администратора 1С.
    :param on_or_off: True - вкл блокировка регламентных заданий, False - откл блокировки регламентных заданий.
    :return: True/False.
    """
    global DEBUG_MODE

    start_time = time.time()
    if DEBUG_MODE:
        info(u'Запуск вкл/откл регламентных заданий информационной базы 1С <%s>' % name)

    result = False

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
                # Устанавливаем режим блокировки регламентных заданий информационной базы 1с
                on_or_off_option = 'on' if on_or_off else 'off'
                cmd = SET_1C_SCHEDULED_JOBS_CMD_FMT % (rac_filename, cluster_id, infobase_id, admin, password, on_or_off_option, host, port)
                set_scheduled_jobs_lines = get_lines_exec_cmd(cmd)
                if DEBUG_MODE and set_scheduled_jobs_lines:
                    for line in set_scheduled_jobs_lines:
                        warning(line.strip())

                # Задержка после выполнения команды
                if SYSTEM_TIME_SLEEP:
                    time.sleep(SYSTEM_TIME_SLEEP)

                # Окончание и выход
                break

    if DEBUG_MODE:
        info(u'Останов вкл/откл регламентных заданий информационной базы 1С <%s> ... %s' % (name, time.time() - start_time))

    return result


if __name__ == '__main__':
    main(*sys.argv[1:])
