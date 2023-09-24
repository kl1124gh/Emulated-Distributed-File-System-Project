from flask import Flask
from flask import render_template
from flask import request
from flask import jsonify
from EDFSClass import EDFS

app = Flask(__name__, template_folder='./template')
file_dir = './dataset/'

@app.route('/', methods=['GET'])
def home():
    file_names = edfs.ls("./dataset")
    return render_template('list.html', file_names = file_names)


@app.route('/get_columns', methods=['GET'])
def get_table_columns():
    request_name = request.args.get('file_name')
    if request_name == '' or request_name is None:
        return jsonify({
            "table_name": "",
            "columns": []
        })
    file_path = file_dir + request_name
    data_list, file_name, path_id = edfs.getDataList(file_path)
    if len(data_list) == 0:
        return jsonify({
            "table_name": "",
            "columns": []
        })
    file_type = file_name.split(".")
    table_name = file_type[0] + "_" + str(data_list[0]["id"])
    columns = edfs.get_tb_columns(table_name)
    return jsonify({
        "table_name": table_name,
        "columns": columns
    })


@app.route('/get_table_data', methods=['GET'])
def get_table_data():
    table_name = request.args.get('table_name')
    columns_name = request.args.get('columns_name')
    condition = request.args.get('condition')
    columns_value = request.args.get('columns_value')

    default_result = jsonify({
        "partitions": [],
        "reduce": []
    })
    if table_name == '' or table_name is None:
        return default_result
    if columns_name == '' or columns_name is None:
        return default_result
    if condition == '' or condition is None:
        return default_result
    if columns_value == '' or columns_value is None:
        return default_result

    if condition == 'contains':
        condition = ' like '
        columns_value = '\'%' + columns_value + '%\''
    else:
        columns_value = '\'' + columns_value + '\''

    where_dic = {
        columns_name + condition : columns_value
    }
    table_data = edfs.query_data_from_table(table_name, where_dic)
    return jsonify(table_data)


if __name__ == '__main__':
    edfs = EDFS("localhost", "root", "Qiqi140624..", 3306)
    app.run(port=8001)