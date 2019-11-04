def parse_cql(cql_text):
    """

    :param cql_text:
    :return:
    """
    cql_text = remove_block_comments(cql_text)

    cmd = []
    for line in cql_text.splitlines():
        if line != "":
            if not line.startswith('--') and not line.startswith("//"):
                cmd.append(line)
    cmd = "\n".join(i for i in cmd)

    return cmd


def remove_block_comments(cql):
    """

    :param cql:
    :return:
    """
    i = 0
    while True:
        start = cql.find("/*")
        if start == -1:
            break
        end = cql.find("*/")
        if end == -1:
            print("ERROR: unclosed block comment")
            break
        cql = cql.replace(cql[start:end + 2], "")
        i = i + 1

    return cql
