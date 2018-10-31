default_processor = lambda x: x

column_name_mappings = {
    0: {
        'name': 'Floor Number',
        'processor': lambda x: x[0] if x else ''
    },
    3: {
        'name': 'Serial'
    },
    4: {
        'name': 'Sensitivity'
    },
    5: {
        'name': 'Bias Level'
    },
    6: {
        'name': 'Orientation'
    },
    7: {
        'name': 'DAQ'
    },
    9: {
        'name': 'X',
        'processor': lambda x: float(x) if x else None
    },
    10: {
        'name': 'Y',
        'processor': lambda x: float(x) if x else None
    },
    11: {
        'name': 'Z',
        'processor': lambda x: float(x) if x else None
    }
}

key_column = 7


def getColumnValue(parts, position):
    processor = (column_name_mappings[position]['processor']
                 if 'processor' in column_name_mappings[position] else default_processor)
    return processor(parts[position])


def parseAccelSheet(fname, has_header=True):
    response = {}
    with open(fname) as f:
        lines = f.readlines()
        lines = list(map(lambda x: x.strip('\r\n'), lines))
        start = (1 if has_header else 0)
        for line in lines[start:]:
            parts = line.split(',')
            key = getColumnValue(parts, key_column)
            if not key:
                continue
            if key not in response:
                response[key] = {}
            num_columns = len(parts)
            for pos in range(num_columns):
                if pos in column_name_mappings and pos != key_column:
                    response[key][column_name_mappings[pos]
                                  ['name']] = getColumnValue(parts, pos)
    return response
