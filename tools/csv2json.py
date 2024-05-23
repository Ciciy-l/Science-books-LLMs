import pandas as pd
import json


def csv2jsonlines(csv_file, json_file):
    """
    将CSV文件转换为JSON文件
    :param csv_file: CSV文件路径
    :param json_file: JSON文件路径
    :return:
    """
    df = pd.read_csv(csv_file)
    df.to_json(json_file, orient="records", lines=True, force_ascii=False)


def jsonlines2xtuner(json_file, xtuner_file, xtuner_type):
    """
    将JSON Lines文件转换为XTuner格式
    :param json_file: JSON Lines文件路径
    :param xtuner_file: XTuner格式文件路径
    :return:
    """

    df = pd.read_json(json_file, lines=True)

    fields_map = {
        "n.BOOK_NAME": "书名",
        "n.ISBN": "书号",
        "n.AUTHOR_NAME": "作者",
        "n.PRICE": "定价",
        "n.OUTLINE": "图书简介",
        "n.PUBLISH_DATE": "出版日期",
        "n.PUBLISH_DEPT": "出版社",
        "n.SUBJECT_HEADINGS": "主题词",
        "n.READER": "推荐读者",
        # "n.RESPONSIBILITY": "责任编辑",
    }

    df["output"] = df.apply(
        lambda row: " ".join(
            [f"{fields_map[col]}: {row[col]}" for col in fields_map if col in row]
        ),
        axis=1,
    )

    if xtuner_type == "incremental":
        final_json = [
            {"conversation": [{"system": "", "input": "", "output": output}]}
            for output in df["output"]
        ]

    elif xtuner_type == "single":
        final_json = [
            {"conversation": [{"system": "", "input": "", "output": output}]}
            for output in df["output"]
        ]

    with open(xtuner_file, "w", encoding="utf-8") as outfile:
        json.dump(final_json, outfile, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    csv_file = "./_temp_datas/books_base_info.csv"
    json_file = "_temp_datas/books_base_info.jsonl"
    xtuner_file = "_temp_datas/books_base_info_xtuner.json"
    # csv2jsonlines(csv_file, json_file)
    jsonlines2xtuner(json_file, xtuner_file)
