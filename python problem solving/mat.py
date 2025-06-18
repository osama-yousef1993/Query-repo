import math


values = input()  # lines
values = values.split(" ")
N = int(values[0])  # rows
dash = "-"
M = int(values[1])  # columns
concat_char = ".|."
concat_char_len = 1
welcome = "WELCOME"
len_welcome = len(welcome)

s = ""
for i in range(1, N + 1):
    mid = int(math.ceil(N / 2))
    if i < mid:
        new_len = len(concat_char * concat_char_len)
        dash_print_len = M - new_len
        print_dash = int(dash_print_len / 2)
        print(f"{dash*print_dash}{concat_char*concat_char_len}{dash*print_dash}")
        # print(f'{dash*print_dash}')
        concat_char_len += 2
        # print the lines above the welcome
    elif i > mid:
        new_len = len(concat_char * concat_char_len)
        dash_print_len = M - new_len
        # print(dash_print_len)
        print_dash = int(dash_print_len / 2)
        print(f"{dash*print_dash}{concat_char*concat_char_len}{dash*print_dash}")
        # # print(f'{dash*print_dash}')
        concat_char_len -= 2
        # print the lines below the welcome
    elif i == mid:
        dash_print_len = M - len_welcome
        print_dash = int(dash_print_len / 2)
        print(f"{dash*print_dash}{welcome}{dash*print_dash}")
        concat_char_len -= 2
        # print welcome
