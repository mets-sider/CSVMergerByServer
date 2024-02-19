import pandas as pd
import os
import datetime
import re
import shutil
import logging

# from stat import filemode


##########################################################################################
# 関数
##########################################################################################
def main():
    global logger
    logger = setup_logging()
    try:
        process_files(in_directories, out_directory_path, backup_directory_path)
    except Exception as e:
        logger.exception(e)


# ファイル処理開始
def process_files(in_directories, out_directory_path, backup_directory_path):
    for in_directory in in_directories:
        all_files = read_directory(in_directory)
        file_count = len(all_files)
        if (in_directory == in_directory2 and file_count == 2) or \
            (in_directory == in_directory3 and file_count == 3):
            # 出力先フォルダ作成
            make_folder(out_directory_path)
            make_folder(backup_directory_path)
            # マージ処理
            merge_files(in_directory, all_files, file_count)

# ログ設定
def setup_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s")
    file_handler = logging.FileHandler(log_directory, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

# ファイル結合
def merge_files(in_directory, all_files, file_count):
    suffix = define_suffix(file_count)
    df_merged = merge(all_files, in_directory)
    ## 一行の場合
    if len(df_merged) == 1:
        merge1row(df_merged, out_directory_path, dt_now_str, suffix)
    split_csv(df_merged, out_directory_path, suffix)
    move(all_files, in_directory)

# サフィックス定義
def define_suffix(file_count):
    if file_count == 2:
        return suffix_simple
    elif file_count == 3:
        return suffix_deadline
    else:
        return None

# ディレクトリチェック
def check_directory(directory):
			if not os.path.isdir(directory):
					logger.warning(f"ディレクトリが存在しません: {directory}")

# ファイルチェック
def check_files(all_files):
    count = 0
    for file in all_files:
        if not os.path.isfile(file):
            logger.warning(f"ファイルが存在しません: {file}")
            count += 1

# ファイル読み込み
def read_directory(in_directory):
    all_files = [
        f
        for f in os.listdir(in_directory)
        if os.path.isfile(os.path.join(in_directory, f))
    ]
    return all_files

# 保存するフォルダを作成
def make_folder(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
    return directory_path

# ファイル結合
def merge(all_files, in_directory):
    matched_request = [file for file in all_files if pattern_request.match(file)]
    matched_detail = [file for file in all_files if pattern_detail.match(file)]

    # ファイルが見つからない場合
    if not matched_request or not matched_detail:
        logger.warning("ファイルが見つかりません。")
        return None

    file1_path = os.path.join(in_directory, matched_request[0])
    print(file1_path)
    file2_path = os.path.join(in_directory, matched_detail[0])
    print(file2_path)
    df1 = pd.read_csv(file1_path, encoding="utf-8")

    df2 = pd.read_csv(file2_path, encoding="utf-8")
    dfs = [df1, df2]

    # 結合処理
    df_merged = pd.merge(dfs[0], dfs[1], on=column_D)
    return df_merged


# 一件（カラム名除く）対応
# DataFrameの行数が1の場合、分割せずに保存して終了
def merge1row(df_merged, out_directory_path, dt_now_str, suffix):
    part_number = 1
    out_folder_path = os.path.join(
        out_directory_path, f"{dt_now_str}_{suffix}_part{part_number}.csv"
    )
    df_merged.to_csv(out_folder_path, index=False)

# 複数件対応
# CSVファイルを条件に基づいて分割する処理
# DataFrameを走査
def split_csv(df_merged, out_directory_path, suffix):
    current_idx = 0
    part_number = 1
    while current_idx < len(df_merged):
        i = current_idx
        for i in range(current_idx + 1, len(df_merged)):
            # 条件をチェック
            # 1.取引先コードが同じ
            # 2.おもての金額（取引金額（税抜）_x）が同じ
            # 3.上記2つの条件が満たされ、かつ発生元伝票番号が異なる
            if (
                df_merged.loc[i, column_A] == df_merged.loc[i - 1, column_A]
                and df_merged.loc[i, column_B] == df_merged.loc[i - 1, column_B]
                and df_merged.loc[i, column_C] == df_merged.loc[i - 1, column_C]
                and df_merged.loc[i, column_D] != df_merged.loc[i - 1, column_D]
            ):
                # 条件を満たす行が見つかった場合、CSVを分割
                out_folder_path = os.path.join(
                    out_directory_path, f"{dt_now_str}_{suffix}_part{part_number}.csv"
                )
                df_merged.iloc[current_idx:i].to_csv(out_folder_path, index=False)
                part_number += 1
                current_idx = i
                break
        # 条件が満たされなかった場合、残りの行を新しいCSVとして分割
        else:
            # 残りの部分を新しいCSVとして保存
            output_path = os.path.join(
                out_directory_path, f"{dt_now_str}_{suffix}_part{part_number}.csv"
            )
            df_merged.iloc[current_idx:].to_csv(output_path, index=False)
            break

# ファイルを移動
def move(all_files, in_directory):
    for filename in all_files:
        file_path = os.path.join(in_directory, filename)
        # 移動処理
        backup_folder_path = os.path.join(backup_directory_path, filename)
        shutil.copy(file_path, backup_folder_path)

##########################################################################################
# 関数終わり
##########################################################################################


##########################################################################################
# メイン処理
##########################################################################################
logger = None

# 現在時刻を取得
dt_now = datetime.datetime.now()
dt_now_str = dt_now.strftime("%Y%m%d%H%M%S")

# サーバで動かす場合
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# 指定ディレクトリ内の全ファイルのリストを取得
log_directory = "log.log"
in_directory2 = "IN_OUT/2in"
in_directory3 = "IN_OUT/3in"
out_directory = "IN_OUT/out"
backup_directory = "backup"
in_directories = [in_directory2, in_directory3]
directories = [in_directory2, in_directory3, out_directory, backup_directory]
out_directory_path = os.path.join(out_directory, dt_now_str)
backup_directory_path = os.path.join(backup_directory, dt_now_str)

request = "請求依頼"
detail = "請求依頼明細"
collection = "請求依頼回収予定"

column_A = "請求先コード"
column_B = "取引金額（税抜）_x"
column_C = "消費税額_x"
column_D = "請求依頼番号"

pattern_request = re.compile(rf"{request}\d+\.csv")
pattern_detail = re.compile(rf"{detail}\d+\.csv")
pattern_collection = re.compile(rf"{collection}\d+\.csv")

suffix_simple = "simple"
suffix_deadline = "deadline"

if __name__ == "__main__":
    main()
