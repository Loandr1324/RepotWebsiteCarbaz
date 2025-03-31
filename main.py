# Author Loik Andrey 7034@balancedv.ru
import time
from typing import Tuple

from loguru import logger
import smbclient
from pandas import DataFrame
import os  # Загружаем библиотеку для работы с файлами
import config
import send_mail  # Универсальный модуль для отправки сообщений на почту
from datetime import date, timedelta  # Загружаем библиотеку для работы с текущим временем
import pandas as pd
import matplotlib.pyplot as plt
import io  # Загружаем библиотеку для работы с директориями
from smbclient import shutil as smb_shutil  # Универсальный модуль для копирования файлов


# Заготовка для записи логов в файл
logger.add(config.FILE_NAME_CONFIG,
           format="{time:DD/MM/YY HH:mm:ss} - {file} - {level} - {message}",
           level="INFO",
           rotation="1 month",
           compression="zip")

# Создаём подключение для работы с файлами на сервере
smbclient.ClientConfig(username=config.LOCAL_PATH['USER'], password=config.LOCAL_PATH['PSW'])
path = config.LOCAL_PATH['PATH_REPORT_SERVER1']

# Наименование подготовленных к отправке по почте файлов с данными и графиком
out_file_custom = 'Carbaz заказы клиентов (статистика).xlsx'
out_file_supp = 'Carbaz Наши_зак_поставщикам наличие (статистика).xlsx'


def get_sms_report():
    """Используем для тестов подключения к папке с отчетами"""
    smbclient.ClientConfig(username=config.LOCAL_PATH['USER'], password=config.LOCAL_PATH['PSW'])
    path1 = config.LOCAL_PATH['PATH_REPORT'] + r"\Исходные данные"
    list_file = []
    try:
        list_file = smbclient.listdir(path1)
        logger.info(f"Получили список файлов с отчётами: {list_file}")
    except ConnectionError:
        logger.error(f"Не могу подключиться к папке с отчетами:")
        logger.error(ConnectionError)
    return list_file


def search_file():
    """
    Поиск нужного файла и добавления в нужный список
    :return: списки с наименованиями фалов заказов клиента, заказов поставщика, поступлений и смс
    f_custom_order, f_supp_order, f_supp_receipt, f_sms
    """

    paths = [
        config.LOCAL_PATH['PATH_REPORT_SERVER1'] + r"\Исходные данные",
        config.LOCAL_PATH['PATH_REPORT_SERVER2'] + r"\Исходные данные",
        config.LOCAL_PATH['PATH_REPORT_SERVER3'] + r"\Исходные данные",
        config.LOCAL_PATH['PATH_REPORT_SERVER4'] + r"\Исходные данные",
        config.LOCAL_PATH['PATH_REPORT_SERVER5'] + r"\Исходные данные",
        config.LOCAL_PATH['PATH_REPORT_SERVER6'] + r"\Исходные данные",
        config.LOCAL_PATH['PATH_REPORT_SERVER7'] + r"\Исходные данные",
        config.LOCAL_PATH['PATH_REPORT_SERVER8'] + r"\Исходные данные",
    ]
    f_custom_order = []
    f_supp_order = []
    f_supp_receipt = []
    f_sms = []
    for path_item in paths:
        for item in smbclient.listdir(path_item):  # для каждого файла в папке folder
            customer_order = item.startswith('Заказы клиентов', 12, 50)
            supplier_order = item.startswith('Заказы поставщиков', 12, 50)
            supplier_receipt = item.startswith('Поступления МХ', 12, 50)
            sms = item.startswith('report_sent', 0, 50)
            if customer_order and item.endswith('.xlsx'):
                f_custom_order.append(path_item + "/" + item)
            elif supplier_order and item.endswith('.xlsx'):
                f_supp_order.append(path_item + "/" + item)
            elif supplier_receipt and item.endswith('.xlsx'):
                f_supp_receipt.append(path_item + "/" + item)
            elif sms and item.endswith('.csv'):
                f_sms.append(path_item + "/" + item)
            else:
                pass
    # Строки для тестов программы
    # logger.info('Файлы клиентов: ' + str(f_custom_order))
    # logger.info('Файлы поставщиков: ' + str(f_supp_order))
    # logger.info('Файл поступления: ' + str(f_supp_receipt))
    # logger.info('Файл смс: ' + str(f_sms))

    return f_custom_order, f_supp_order, f_supp_receipt, f_sms


def send_mail_error(file_custom_order: list, file_supp_order: list, file_supp_receipt: list, file_sms: list) -> None:
    """
    Отправляем сообщения об ошибке при выполнении программы
    :param file_custom_order: Список файлов по Заказам клиентов
    :param file_supp_order: Список файлов по Заказам поставщикам
    :param file_supp_receipt: Список файлов по Поступлениям МХ
    :param file_sms: Список файлов по Отправкам СМС
    :return: None
    """
    message = {
        'Subject': f"Ошибка при формировании ежемесячного отчета по Carbaz",
        'email_content': '',
        'To': config.TO_EMAILS['TO_ERROR'],
        'File_name': '',
        'Temp_file': ''
    }
    if not file_custom_order:
        logger.info(f"Нет отчета по Заказам клиента за предыдущий месяц.")
        message['email_content'] = (f"Нет отчета по Заказам клиента за предыдущий месяц.<br>"
                                    f"Разместите отчет в папке:<br>"
                                    f"{config.LOCAL_PATH['PATH_REPORT']}")
    if not file_supp_order:
        logger.info(f"Нет отчета по Заказам поставщика за предыдущий месяц.")
        message['email_content'] = (f"Нет отчета по Заказам поставщика за предыдущий месяц.<br>"
                                    f"Разместите отчет в папке:<br>"
                                    f"{config.LOCAL_PATH['PATH_REPORT']}")
    if not file_supp_receipt:
        logger.info(f"Нет отчета по Поступлениям от МХ за предыдущий месяц.")
        message['email_content'] = (f"Нет отчета по Поступлениям от МХ за предыдущий месяц.<br>"
                                    f"Разместите отчет в папке:<br>"
                                    f"{config.LOCAL_PATH['PATH_REPORT']}")
    if not file_sms:
        logger.info(f"Нет отчета по отправкам СМС за предыдущий месяц.")
        message['email_content'] = (f"Нет отчета по отправкам СМС за предыдущий месяц.<br>"
                                    f"Разместите отчет в папке:<br>"
                                    f"{config.LOCAL_PATH['PATH_REPORT_SERVER1']} или "
                                    f"{config.LOCAL_PATH['PATH_REPORT_SERVER2']}")
    # Оправка письма со сформированными параметрами
    send_mail.send(message)
    return


def rename_out_file():
    """
    Определяем текущий месяц и переименовываем файлы, в которые будем добавлять данные
    Вызывается из функции search_append_custom
    Возвращаем новое имя файла
    :return: new_file_custom, new_file_supp
    """
    # Определяем дату для наименования нового файла
    date_new_name = (date.today() - timedelta(days=28)).strftime('%m.%Y')
    # Определяем дату для переименования старого файла
    date_old_name = (date.today() - timedelta(days=58)).strftime('%m.%Y')

    # Полное наименование нового файла клиентов
    new_file_custom = f'Carbaz Заказы клиентов (до {date_new_name}).xlsx'
    # Полное наименование нового файла поставщиков
    new_file_supp = f'Carbaz Наши_зак_поставщикам_наличие (до {date_new_name}).xlsx'
    # Полное наименование старого файла клиентов
    old_file_custom = f'Carbaz Заказы клиентов (до {date_old_name}).xlsx'
    # Полное наименование старого файла поставщиков
    old_file_supp = f'Carbaz Наши_зак_поставщикам_наличие (до {date_old_name}).xlsx'
    '''
    Переименовываем старые файлы
    '''
    # TODO Раскомментировать строки после тестов
    os.rename(old_file_custom, new_file_custom)  # rename old file custom
    os.rename(old_file_supp, new_file_supp)  # rename old file supp

    return new_file_custom, new_file_supp


def read_xlsx_custom(file_list, file_list_sms):
    """
    Загружаем в DataFrame файлы Заказов клиента и смс эксель
    :param file_list_sms:
    :param file_list:
    :return: custom_row, sms_row
    """
    # custom_row = pd.DataFrame()
    # for filename in file_list:
    #     with smbclient.open_file(filename, 'rb') as s:
    #         df = pd.read_excel(s, header=10, usecols='A:M', skipfooter=1, engine='openpyxl')
    #     custom_row = custom_row.concat(df, ignore_index=True)
    custom_row = pd.concat([pd.read_excel(smbclient.open_file(filename, 'rb'), header=10, usecols="A:M",
                                          skipfooter=1, engine='openpyxl') for filename in file_list],
                           ignore_index=True)
    custom_row = custom_row.dropna(axis=1, how='all')  # Удаление пустых колонок, если axis=0, то строк
    sms_row = pd.concat([pd.read_csv(smbclient.open_file(filename, 'rb'), sep=';', index_col=False,
                                     encoding='utf-8') for filename in file_list_sms], ignore_index=True)
    sms_row = sms_row.dropna(axis=1, how='all')  # Удаление пустых колонок, если axis=0, то строк
    # Это общее количество строк для Юры. Надо будет подумать как ему передать
    quantity_row_custom = len(custom_row)
    # print('Количество строк в заказах клиента: ', quantity_row_custom)
    return custom_row, sms_row, quantity_row_custom  # for_ura_custom


def read_xlsx_supp(file_list_order: list, file_list_receipt: list) -> tuple[DataFrame, DataFrame, int]:
    """
    Read exel ШАВ, ШСВ, TC
    Читаем файлы поставщиков заказов, поступлений по всем организациям и складываем все данные в DateFrame
    :return: supp_or_row, supp_rec_row
    """
    # noinspection PyTypeChecker
    supp_ord_row = pd.concat([
        pd.read_excel(
            smbclient.open_file(filename, 'rb'),
            header=10, usecols="A:J", skipfooter=1, engine='openpyxl'
        ) for filename in file_list_order
    ], ignore_index=True)
    supp_ord_row = supp_ord_row.dropna(axis=1, how='all')  # Удаление пустых колонок, если axis=0, то строк
    # noinspection PyTypeChecker
    supp_rec_row = pd.concat([pd.read_excel(smbclient.open_file(filename, 'rb'),
                                            header=8, usecols="A:I",
                                            skipfooter=0, engine='openpyxl') for filename in file_list_receipt],
                             ignore_index=True)
    supp_rec_row = supp_rec_row.dropna(axis=1, how='all')  # Удаление пустых колонок, если axis=0, то строк
    # Это общее количество строк для Юры. Надо будет подумать как ему передать
    quantity_row_supp = len(supp_ord_row) + len(supp_rec_row)
    # print('Количество строк в заказах поставщика: ', for_ura_supp)
    return supp_ord_row, supp_rec_row, quantity_row_supp  # for_ura_supp


def sorting_custom_row(custom_row_for_sort):
    """
    Сортируем список по трем категориям:
    1. Продажи со своего склада
    2. Продажи с других складов
    3. Товары на заказ
    4. Так же нужно будет передать в следующую функцию общий список для суммирования у Юры
    :param custom_row_for_sort:
    :return: your_warehouse, another_warehouse, by_order
    """
    conformity_wh = (['01 Кирова', '01'],
                     ['02 Автолюбитель', 'al02'],
                     ['03 Интер', 'in03'],
                     ['04 Победа', 'pd04'],
                     ['05 Павловский', 'mx'],
                     ['05 Павловский', 'pl05'],
                     ['08 Центр', 'cn08'],
                     ['09 Вокзалка', 'vz09'])
    '''
    Ищем строки на заказ
    '''
    mask_by_order = custom_row_for_sort['Заказ клиента.Carbaz order type'] == 'byorder'
    by_order = custom_row_for_sort[mask_by_order]  # DataFrame  со строками на заказ (для вывода в эксель)
    in_order = custom_row_for_sort[~mask_by_order]  # DataFrame со строками в наличии (для дальнейшей сортировки)
    '''
    Сортируем строки со своего склада
    '''
    mask_your_warehouse = pd.Series(False, index=in_order.index)
    for i in conformity_wh:
        mask_wh_cl = in_order['Склад'] == i[0]
        mask_wh_sh = in_order['Carbaz goods supplier text'].str.contains(i[1])
        mask_wh = mask_wh_cl & mask_wh_sh
        mask_your_warehouse = mask_your_warehouse | mask_wh
    your_warehouse = in_order[mask_your_warehouse]
    '''
    Оставшиеся строки в наличии с другого склада
    '''
    another_warehouse = in_order[~mask_your_warehouse]

    '''
    Записываем в эксель файлы. Используется при тестах, для получение промежуточных данных.
    '''
    # TODO Закомментировать после тестов
    """your_warehouse.to_excel('your_warehouse.xlsx')
    another_warehouse.to_excel('another_warehouse.xlsx')
    by_order.to_excel('by_order.xlsx', index=False)
    custom_row_for_sort.to_excel('custom_row_for_sort.xlsx')"""

    '''
    Общее количество в заказах клиента для Юры
    '''

    return your_warehouse, another_warehouse, by_order


def sorting_sms(sms_row):
    """
    Подсчет количества отправленных СМС
    подсчет СМС  по сумме в колонке SMS_RES_COUNT при условии, что в колонке SMSSTC_CODE статус delivered
    :param sms_row:
    :return: sms - type(<class 'numpy.int64'>)
    """
    """
    Убрал этот код, т..к получил информацию, что мы оплачиваем за все СМС, в том числе не доставленные
    mask_sms = sms_row['SMSSTC_CODE'] == 'delivered'  # Ищем строки, в которых доставленные СМС
    sms_row = sms_row[mask_sms]  # Формирует DataFrame в которых строки только доставленные СМС
    """
    mask_sms = sms_row['Отправитель'] == 'CarBaz'
    sms_row = sms_row[mask_sms]
    sms = sms_row['Количество'].sum()  # Суммируем количество СМС
    return sms


def date_xlsx():
    """
    Определяем необходимые даты в том числе русское строковое наименование месяца для добавления в эксель
    :return: month_name_str, - наименование на русском прошлого месяца январь, ..., декабрь.
    :return: year, - числовое обозначение года прошлого месяца, четыре цифры. Например, 2020.
    :return: month_name_int - числовое обозначение прошлого месяца двухзначное число 01...12.
    """
    ru_month_values = {
        '01': 'Январь',
        '02': 'Февраль',
        '03': 'Март',
        '04': 'Апрель',
        '05': 'Май',
        '06': 'Июнь',
        '07': 'Июль',
        '08': 'Август',
        '09': 'Сентябрь',
        '10': 'Октябрь',
        '11': 'Ноябрь',
        '12': 'Декабрь'
    }
    last_month = date.today() - timedelta(days=25)
    month_name_str = ru_month_values[last_month.strftime('%m')]
    month_name_int = last_month.strftime('%m')
    year = last_month.strftime('%Y')
    return month_name_str, year, month_name_int


def total_df_custom(df1, df2, df3, int1):
    """
    Получение итоговых значений
    :param df1: Продажи с других складов
    :param df2: Продажи со своего склада
    :param df3: Заказное
    :param int1: Количество СМС
    :return: total_custom - type(DataFrame)
    TODO: Надо оптимизировать код, чтобы пройти в цикле и заполнить DataFrame
    """
    type_sales = ['Продажи с других складов', 'Продажи со своего склада', 'Заказное', 'Отправка СМС']
    month_name_str, year = date_xlsx()[:2]
    total_custom = pd.DataFrame({'Тип продажи': [],
                                 'Год': [],
                                 'Месяц': [],
                                 'Количество строк': [],
                                 'Количество товаров': [],
                                 'Сумма': []})
    total_custom = total_custom._append({'Тип продажи': type_sales[0],
                                         'Год': int(year),
                                         'Месяц': month_name_str,
                                         'Количество строк': df1['Уровень в группе'].sum(),
                                         'Количество товаров': df1['Количество (в единицах хранения)'].sum(),
                                         'Сумма': df1['Сумма'].sum()}, ignore_index=True)
    total_custom = total_custom._append({'Тип продажи': type_sales[1],
                                         'Год': int(year),
                                         'Месяц': month_name_str,
                                         'Количество строк': df2['Уровень в группе'].sum(),
                                         'Количество товаров': df2['Количество (в единицах хранения)'].sum(),
                                         'Сумма': df2['Сумма'].sum()}, ignore_index=True)
    total_custom = total_custom._append({'Тип продажи': type_sales[2],
                                         'Год': int(year),
                                         'Месяц': month_name_str,
                                         'Количество строк': df3['Уровень в группе'].sum(),
                                         'Количество товаров': df3['Количество (в единицах хранения)'].sum(),
                                         'Сумма': df3['Сумма'].sum()}, ignore_index=True)
    total_custom = total_custom._append({'Тип продажи': type_sales[3],
                                         'Год': int(year),
                                         'Месяц': month_name_str,
                                         'Количество строк': int1,
                                         'Количество товаров': None,
                                         'Сумма': None}, ignore_index=True)
    return total_custom


def total_df_supp(df1, df2):
    """
    Получение итоговых значений
    :param df1: Заказы внешним поставщикам
    :param df2: Поступления от МХ Комсомольск
    :return: total_custom - type(DataFrame)
    TODO: Надо оптимизировать код, чтобы пройти в цикле и заполнить DataFrame
    """
    type_doc = ['Заказы внешним поставщикам', 'Поступления от МХ Комсомольск']
    month_name_str, year = date_xlsx()[:2]
    total_custom = pd.DataFrame({'Тип документа': [],
                                 'Год': [],
                                 'Месяц': [],
                                 'Количество строк': [],
                                 'Количество товаров': [],
                                 'Сумма': []})
    total_custom = total_custom._append({'Тип документа': type_doc[0],
                                         'Год': int(year),
                                         'Месяц': month_name_str,
                                         'Количество строк': df1['Уровень в группе'].sum(),
                                         'Количество товаров': df1['Количество (в единицах хранения)'].sum(),
                                         'Сумма': df1['Сумма'].sum()}, ignore_index=True)
    total_custom = total_custom._append({'Тип документа': type_doc[1],
                                         'Год': int(year),
                                         'Месяц': month_name_str,
                                         'Количество строк': df2['Уровень в группе'].sum(),
                                         'Количество товаров': df2['Товары.Количество (в единицах хранения)'].sum(),
                                         'Сумма': df2['Товары.Сумма с НДС'].sum()}, ignore_index=True)
    return total_custom


def append_file_data(file_name, df_t):
    """
    1. Чтобы получить стороковое наименования файла используем функию date_xlsx()
    4. Отформатировать таблицу согласно общего стиля файла
    :param file_name: Название файла с данными для записи
    :param df_t: DataFrame для добавления к данным
    :return: None
    """
    # Определение строки для записи
    start_row = len(pd.read_excel(file_name, sheet_name='Данные', engine='openpyxl')) + 1
    # Дописать в итоговый файл с данными, для дальнейшей обработки полученные строки
    with pd.ExcelWriter(file_name, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
        df_t.to_excel(writer, 'Данные', index=False, header=False, startrow=start_row, startcol=0)


def pivot_table(filename, category, set_cat, index):
    """
    Перевод из файла данных(в который дописываем строки) по всем месяцам в сводную таблицу
    :param filename: Имя файла данных для чтения
    :param category: Категория для разных данных
    :param set_cat: Занчения категорий
    :param index: Индекс для выходной таблицы
    :return:
    """
    # noinspection PyTypeChecker
    data_pd = pd.read_excel(filename, sheet_name='Данные', header=0, usecols="A:F",
                            skipfooter=0, engine='openpyxl')
    data_pd[category] = data_pd[category].astype('category')
    data_pd[category] = data_pd[category].cat.set_categories(set_cat,
                                                             ordered=True)
    data_pd['Месяц'] = data_pd['Месяц'].astype('category')
    data_pd['Месяц'] = data_pd['Месяц'].cat.set_categories(['Январь', 'Февраль', 'Март',
                                                            'Апрель', 'Май', 'Июнь',
                                                            'Июль', 'Август', 'Сентябрь',
                                                            'Октябрь', 'Ноябрь', 'Декабрь',
                                                            'Итого по типу'],
                                                           ordered=True)
    data_pt = pd.pivot_table(data_pd, index=index, aggfunc='sum',
                             values=['Количество строк', 'Количество товаров', 'Сумма'], fill_value=0, sort=False)
    return data_pt


def result_to_xlsx(exel_file, data_pt, category, caption, sec_level, end_level):
    """
    Запись результата в эксель
    :param end_level:
    :param sec_level:
    :param caption:
    :param category:
    :param exel_file:
    :param data_pt:
    :return:
    """
    sheet_name1 = 'Данные'  # Наименование вкладки для сводной таблицы
    sheet_name2 = 'Графики'  # Наименование вкладки для графиков
    writer = pd.ExcelWriter(exel_file, engine='xlsxwriter')  # Открываем файл для записи
    workbook = writer.book
    # Записываем данные на вкладку данные
    df_write_xlsx(writer, sheet_name1, workbook, data_pt, caption, sec_level, end_level)
    # Добавление графика на вкладке Графики
    imgdata = plotting(data_pt, category)
    plot_write_xlsx(workbook, sheet_name2, imgdata)
    writer._save()
    return


def plotting(data_pt, category):
    """
    Переформирование сводной таблицы и построение графика
    :param category: Тип категории,
    :param data_pt: Сводная таблица с данными для построениея графика
    :return: imgdata
    """
    '''
    Подготовка DataFrame для построения графика
    '''
    try:
        data_pt = data_pt.drop('Отправка СМС', level=1)
    except:
        pass
    data_plot = data_pt.reset_index(level=category, drop=True)  # Удаление индекса Типа категории
    data_plot = data_plot.groupby(level=['Год', 'Месяц'],
                                  observed=False).sum()  # Сумма при одинаковых занчения года и месяца
    data_plot = data_plot.unstack('Год')  # Перевод индекса строк Год в строку Год на верх таблицы
    '''
    Построение и запись графика
    '''
    # Создание области для построения из двух строк и одной колонки с размером 12*8 для графиков
    fig, ax = plt.subplots(3, 1, figsize=(12, 10))
    # Построение графика по Количеству строк в перовой обласи ax[0]
    data_plot.plot(y='Количество строк', kind='bar', rot=0, ylabel='Количество строк, шт.', alpha=0.7, width=0.8,
                   ax=ax[0])
    # Построение графика по Сумме во второй обласи ax[1]
    data_plot.plot(y='Сумма', kind='bar', rot=0, ylabel='Сумма, млн.руб.', alpha=0.7, width=0.8,
                   ax=ax[1])
    # Построение графика по Количеству товара во второй обласи ax[1]
    data_plot.plot(y='Количество товаров', kind='bar', rot=0, ylabel='Количество товаров, шт.', alpha=0.7, width=0.8,
                   ax=ax[2])
    # Установка легенды справа на графике(изменил на лево)
    ax[0].legend(loc='upper right')
    ax[1].legend(loc='upper right')
    ax[2].legend(loc='upper right')
    # Отрисовка сетки по оси y на двух графиках
    ax[0].grid(axis='y', linestyle='--', color='gray')
    ax[1].grid(axis='y', linestyle='--', color='gray')
    ax[2].grid(axis='y', linestyle='--', color='gray')
    # Вывод графика на экран - используется для тестов
    # plt.show()
    # Запись графика в память
    imgdata = io.BytesIO()
    fig.savefig(imgdata, format="png")
    return imgdata


def df_write_xlsx(writer, sheet_name, workbook, data_pt, caption, sec_level, end_level):
    """
    Переработка DataFrame и запись в эксель данных
    :param sec_level: Второй уровень группировок Год и второй индек Тип продажи (клиенты) или Месяц(поставзики
    :param end_level: Индексы конечного уровня группировок для суммирования(все индексы)
    :param caption:
    :param data_pt:
    :param workbook:
    :param sheet_name:
    :param writer:
    :return: запись эксель файл
    """
    # Получаем словари форматов для эксель
    year_format, caption_format, sales_type_format, month_format, sum_format, quantity_format = format_custom(workbook)
    # Получаем сумму по колонкам по месяцам в каждом году
    data_pt1 = data_pt.groupby(level=sec_level, observed=False).sum()
    # Получаем сумму по всем колокам по всем типам за год. Предварительно удаляем данные по Отправке СМС
    try:
        data_pt2 = data_pt.drop('Отправка СМС', level=1).sum(level=['Год'])
    except:
        data_pt2 = data_pt.groupby(level=['Год']).sum()
    # Получаем сумму по колонкам по месяцам в каждом году и по типу документа,
    # чтобы избежать ошибки, что нет Отправки СМС
    data_pt3 = data_pt.groupby(level=end_level, observed=False).sum()
    start_row = 4  # Задаём первую строку для записи таблицы с данными
    for i in data_pt3.index.unique(level=0):  # Цикл по годам
        df = data_pt2.loc[[i]]  # Создаём DataFrame по каждому году для записи
        # Записываем данные по каждому году в эксель
        df.to_excel(writer, sheet_name=sheet_name, startrow=start_row, header=False)
        wks1 = writer.sheets[sheet_name]  # Открываем вкладку для форматирования
        wks1.set_column('A:A', 30, None)  # Изменяем ширину первой колонки где расположен Год, Тип продажи и месяц
        wks1.set_column('B:B', 10, quantity_format)  # Изменяем ширину и формат колнки с количеством строк
        wks1.set_column('C:C', 10, quantity_format)  # Изменяем ширину и формат колнки с количеством товаров
        wks1.set_column('D:D', 18, sum_format)  # Изменяем ширину и формат колнки с суммой
        # Поскольку формат индекса изменить нельзя, то перезаписываем наименование каждого года и меняем формат
        wks1.write(f'A{start_row + 1}', i, year_format)
        # Изменяем формат всей строки для каждого года с данными о количестве и сумме
        wks1.conditional_format(f'A{start_row + 1}:D{start_row + 1}',
                                {'type': 'no_errors', 'format': year_format})
        wks1.set_row(start_row, 20, None)  # Изменяем высоту каждой строки с годом
        start_row += len(data_pt2.loc[[i]])  # Изменяем значение стартовой строки для следующих записей
        for k in data_pt3.index.unique(level=1):  # Цикл по месяцам
            # Записываем данные по каждому типу продаж для каждого года в эксель
            data_pt1.loc[[(i, k)]].droplevel(level=0).to_excel(writer, sheet_name=sheet_name, startrow=start_row,
                                                               header=False)
            # Добавляем группировку по году, данные по месяцам не скрываем
            wks1.set_row(start_row, None, None, {'level': 1})
            # Поскольку формат индекса изменить нельзя, то перезаписываем наименование каждого
            # месяца и меняем формат
            wks1.write(f'A{start_row + 1}', k, sales_type_format)
            # Изменяем формат всей строки для каждого типа продаж с данными о количестве и сумме
            wks1.conditional_format(f'A{start_row + 1}:D{start_row + 1}',
                                    {'type': 'no_errors', 'format': sales_type_format})
            start_row += len(data_pt1.loc[[(i, k)]])  # Изменяем значение стартовой строки для следующих записей
            # Записываем данные сразу по всем месяцам каждого года и каждого типа продаж в эксель
            data_pt3.loc[(i, k)].to_excel(writer, sheet_name=sheet_name, startrow=start_row, header=False)
            # Добавляем группировку по типу продаж, данные по месяцам скрываем
            for n in range(start_row, start_row + len(data_pt3.loc[(i, k)])):
                if i == data_pt3.index.unique(level=0)[-1]:
                    wks1.set_row(n, None, None, {'level': 2, 'hidden': False})
                else:
                    wks1.set_row(n, None, None, {'level': 2, 'hidden': True})
            # Поскольку формат индекса изменить нельзя, то перезаписываем наименование каждого
            # месяца и меняем формат
            for m in data_pt3.index.unique(level=2):  # Цикл по месяцам
                wks1.write(f'A{start_row + 1}', m, month_format)
                start_row += 1
        # Запись и формат заголовка таблицы
        wks1.write('A2', caption, caption_format)
        # Добавление отображение итогов группировок сверху
        wks1.outline_settings(True, False, False, False)
    return


def plot_write_xlsx(workbook, sheet_name, imgdata):
    workbook.formats[0].set_font_size(9)
    wks2 = workbook.add_worksheet(sheet_name)  # Добавление ещё одной вкладки
    wks2.insert_image(0, 0, '', {'image_data': imgdata})  # Вставка картинки с рисунком из памяти
    # wks2.set_first_sheet()
    wks2.activate()


def format_custom(workbook):
    year_format = workbook.add_format({
        'font_name': 'Arial',
        'font_size': '10',
        'align': 'left',
        'bold': True,
        'bg_color': '#F4ECC5',
        'border': True,
        'border_color': '#CCC085'
    })
    sales_type_format = workbook.add_format({
        'font_name': 'Arial',
        'font_size': '8',
        'align': 'left',
        'border': True,
        'border_color': '#CCC085',
        'bg_color': '#F8F2D8'
    })
    month_format = workbook.add_format({
        'font_name': 'Arial',
        'font_size': '8',
        'align': 'right',
        'bold': False,
        'border': True,
        'border_color': '#CCC085'
    })
    sum_format = workbook.add_format({
        'num_format': '# ### ##0.00"р.";[red]-# ##0.00"р."',
        'font_name': 'Arial',
        'font_size': '8',
        'border': True,
        'border_color': '#CCC085'
    })
    quantity_format = workbook.add_format({
        'num_format': '# ### ##0',
        'font_name': 'Arial',
        'font_size': '8',
        'border': True,
        'border_color': '#CCC085'
    })
    caption_format = workbook.add_format({
        'font_name': 'Arial',
        'font_size': '14',
        'bold': True,
        'border': True,
        'border_color': '#CCC085'
    })

    return year_format, caption_format, sales_type_format, month_format, sum_format, quantity_format


def send_file_to_mail(files: list, quantity_row_custom=None, quantity_row_supp=None):
    """
    Отправляем файл на почту
    :param quantity_row_custom: Кол-во строк в заказах клиента
    :param quantity_row_supp: Кол-во строк в заказах поставщику
    :param files: -> str - Имя файла для отправки
    :return:
    """
    # Получаем месяц и год из функции по определению дат
    year, month = date_xlsx()[1:]
    # Текст сообщения в формате html
    email_content = f"""
        <html>
          <head></head>
          <body>
            <p>
                Добавил в отчеты данные за {month}.{year}г.<br>
                Отчеты во вложении.<br>
                Юра, общее по строкам:<br>
                Количество строк в заказах клиента:
                &emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&ensp;&nbsp;
                <b>{quantity_row_custom}</b>
                <br>
                Количество строк в заказах внешним поставщикам:
                &emsp;
                <b>{quantity_row_supp}</b>
            </p>
          </body>
        </html>
    """
    message = {
        'Subject': f'Carbaz отчеты {month}.{year}',  # Тема сообщения,
        'email_content': email_content,
        'To': config.TO_EMAILS['TO_CORRECT'],
        'File_name': [
            'Carbaz заказы клиентов (статистика).xlsx',
            'Carbaz Наши_зак_поставщикам наличие (статистика).xlsx'
        ],
        'Temp_file': files

    }

    send_mail.send(message)
    return


def remove_files():
    """
    Копируем отчеты с исходной папки и файлы с данными в папку за месяц и удаляем все файлы из исходной папки с отчётами
    :return: None
    """
    paths = [
        config.LOCAL_PATH['PATH_REPORT_SERVER1'],
        config.LOCAL_PATH['PATH_REPORT_SERVER2'],
        config.LOCAL_PATH['PATH_REPORT_SERVER3'],
        config.LOCAL_PATH['PATH_REPORT_SERVER4'],
        config.LOCAL_PATH['PATH_REPORT_SERVER5'],
        config.LOCAL_PATH['PATH_REPORT_SERVER6'],
        config.LOCAL_PATH['PATH_REPORT_SERVER7'],
        config.LOCAL_PATH['PATH_REPORT_SERVER8'],
    ]
    # path1 = config.LOCAL_PATH['PATH_REPORT_SERVER1'] + r"\Исходные данные"
    year, month = date_xlsx()[1:]
    path2 = path + f"/Исходные данные на {month}.{year}"

    # Переносим файлы из Исходной директории в резервную
    for path_item in paths:
        path1 = path_item + r"\Исходные данные"
        path2 = path_item + f"/Исходные данные на {month}.{year}"

        # Создаём резервную папку за месяц отчета
        smbclient.mkdir(path2)

        for item in smbclient.listdir(path1):
            smbclient.copyfile(path1 + "/" + item, path2 + "/" + item)
            smbclient.remove(path1 + "/" + item)
    # Переносим файлы с данными из директории скрипта в резервную папку
    for item in os.listdir():
        if item.endswith('.xlsx'):
            smb_shutil.copyfile(item, path2 + "/" + item)
    return


def run():
    """
    Весь рабочий процесс программы по подстановке менеджера в заказы без менеджера.
    """
    logger.info(f"... Начало работы программы")
    logger.info(f"Начало Блока №1")
    # Поиск списков файлов для чтения и распределение по типам файлов
    file_custom_order, file_supp_order, file_supp_receipt, file_sms = search_file()
    # Отправляем сообщение об ошибках, если каких-то файлов не хватает
    if not file_custom_order or not file_supp_order or not file_supp_receipt or not file_sms:
        send_mail_error(file_custom_order, file_supp_order, file_supp_receipt, file_sms)
        return
    # Определение старых и новых имён файлов с хранимыми данным и переименование
    file_custom, file_supp = rename_out_file()
    # Чтение данных за прошедший месяц по заказам клиента и запись в DataFrame + получения общего количества строк
    custom_row, sms_row, quantity_row_custom = read_xlsx_custom(file_custom_order, file_sms)
    # Сортировка DataFrame заказов клиента по типам продаж
    your_warehouse, another_warehouse, by_order = sorting_custom_row(custom_row)
    # Сортировка DataFrame СМС. Добавляем, если доставлено
    sms = sorting_sms(sms_row)
    # Чтение данных за прошедший месяц по заказам поставщикам и запись в DataFrame + получения общего количества строк
    supp_ord_row, supp_rec_row, quantity_row_supp = read_xlsx_supp(file_supp_order, file_supp_receipt)
    # Получение итоговых значений по типам продаж и объединение итоговых значений в один DataFrame
    total_custom = total_df_custom(another_warehouse, your_warehouse, by_order, sms)
    # Получение итоговых значений по типам документов и объединение итоговых значений в один DataFrame
    total_supp = total_df_supp(supp_ord_row, supp_rec_row)
    # Добавляем в файл данных по клиентам, данные по Заказам клиента
    append_file_data(file_custom, total_custom)
    # Добавляем в файл данных по поставщикам, данные по Заказам поставщикам
    append_file_data(file_supp, total_supp)
    logger.info("Блок №1 завершён!")
    '''
    Блок №2
    Чтение данных из файлов данных, построение графиков и запись итоговых файлов для отправки по почте
    '''
    # Считываем данные за всё время и формируем итоговый файл по Заказам для наличия
    logger.info("Начало Блока №2")
    # print(file_custom, file_supp) # Используем при тестах
    custom_category = 'Тип продажи'
    custom_set_cat = ['Продажи с других складов', 'Продажи со своего склада',
                      'Заказное', 'Отправка СМС', 'Итого за год']
    custom_index = ['Год', 'Тип продажи', 'Месяц']
    supp_category = 'Тип документа'
    supp_set_cat = ['Заказы внешним поставщикам', 'Поступления от МХ Комсомольск', 'Итого за год']
    supp_index = ['Год', 'Тип документа', 'Месяц']
    supp_index_out = ['Год', 'Месяц', 'Тип документа']
    caption_custom = 'Carbaz Заказы клиентов. Все заказы кроме наших заказов внешним поставщикам на ' \
                     'постоянное наличие. Включая индивидуальные заказы.'
    caption_supp = 'Наши заказы внешним поставщикам на постоянное наличие. (Заказы Клиентов не входят)'
    sec_level_custom = ['Год', 'Тип продажи']
    sec_level_supp = ['Год', 'Месяц']
    data_pt_custom = pivot_table(file_custom, custom_category, custom_set_cat, custom_index)
    data_pt_supp = pivot_table(file_supp, supp_category, supp_set_cat, supp_index)
    result_to_xlsx(out_file_custom, data_pt_custom, custom_category, caption_custom, sec_level_custom, custom_index)
    result_to_xlsx(out_file_supp, data_pt_supp, supp_category, caption_supp, sec_level_supp, supp_index_out)
    logger.info("Блок №2 завершён!")
    '''
    Блок №3
    Отправка файлов по почте и выдержка по количеству строк в теле письма
    '''
    # Подготавливаем и отправляем данные по почте Юре
    logger.info("Начало Блока №3 - Отправка на почту")
    send_file_to_mail([out_file_custom, out_file_supp], quantity_row_custom, quantity_row_supp)
    logger.info("Блок №3 завершён!")
    logger.info("Начало Блока №4 - Перенос файлов в папку архива и удаление Исходных данных")
    remove_files()
    logger.info("Блок №4 завершён!")

    logger.info(f"... Завершение работы программы")

    return


if __name__ == '__main__':
    run()
