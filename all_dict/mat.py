import math
values = input()  # lines
values = values.split(" ")
N = int(values[0])
M = int(values[1])  # columns
dash = '-'
concat_char = '|'
dot_char = '.'
concat_dot = f'{dot_char}{concat_char}{dot_char}'
len_concat_dot = len(concat_dot)
welcome = "WELCOME"
len_welcome = len(welcome)

s = ''
for i in range(N):
    mid = math.ceil(N/2)  # mid value for N len
    if i < mid:
        print_len = int((M - len_concat_dot) / 2)
        new_len_concat_dot = int(len_concat_dot/3)
        s += f'{dash * print_len}{new_len_concat_dot * concat_dot}{dash * print_len}\n'
        new_len_concat_dot += 2
    # if i == mid :
    #     print_len = M - len_welcome
    #     s += f'{dash * print_len}WELCOME{dash * print_len}\n'
    # if i > mid:
    #     print_len = M - concat_char_len
    #     s += f'{dash * print_len}{concat_char_len * concat_char}{dash * print_len}\n'
    #     concat_char_len -= 2
print(s)

# ---------.|.---------
# ------.|..|..|.------
# ---------.|.---------
# ------.|..|..|.------

