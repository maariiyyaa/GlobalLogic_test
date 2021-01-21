from functools import reduce

def multiply(x: int,y: int) -> int:
    return x*y

def find_numbers(begin: int, end: int) -> list:
    """
    :param begin: range start, int
    :param end: range end, int
    :return: list of numbers where number %7==0 and number %5 != 0

    >>> find_numbers(5, 15)
    [7, 14]
    """
    query_numbers = []
    for number in range(begin, end):
        if (number %7 == 0 and number %5 != 0):
            query_numbers.append(number)
    return query_numbers

def create_dict(span: int) -> dict:
    """
    :param span: range end, int
    :return: dictionary like {i: i*i}

    >>> create_dict(3)
    {1: 1, 2: 4, 3: 9}
    """
    if span < 0:
        print('A value for dictionary should be >0')
        return
    dictionary = dict()
    for i in range(1, span+1):
        dictionary[i] = pow(i, 2)
    return dictionary


def factorial(number: int) -> int:
    """
    :param number: input number, int
    :return: factorial(number), int

    >>> factorial(8)
    40320
    >>> factorial(-2)
    A value for factorial should be >= 0
     """
    if number < 0:
        print('A value for factorial should be >= 0')
        return
    if (number == 0 or number == 1):
        return 1
    return number * factorial(number-1)

def factorial_1(number: int) -> int:
    """
    :param number: input number, int
    :return: factorial(number), int

    >>> factorial(-1)
    A value for factorial should be >= 0
    >>> factorial(0)
    1
     """
    if number < 0:
        print('A value for factorial should be >= 0')
        return
    if (number == 0 ):
        number = 1
    return reduce(multiply, range(1,number+1))

def main():
    # task one version_1
    print('\nTASK_1 VERSION_1')
    print(*find_numbers(2000, 3201), sep = ', ', end='')
    # task one version_2
    print('\n\nTASK_1 VERSION_2')
    print(*list(filter(lambda x: x%7==0 and x%5!=0, range(2000, 3201))), sep=', ', end='')

    print('\n Enter a number for task 2 and task 3:')
    n = int(input())
    # task two
    print('\nTASK_2:\n')
    print(create_dict(n))
    # task three version_1
    print(f'\nTASK_3 VERSION_1\n')
    print(f'factorial({n}) = {factorial(n)}')
    #task three version_2
    print(f'\nTASK_3 VERSION_2\n')
    print(f'factorial({n}) = {factorial_1(n)}')

if __name__ == '__main__':
    main()
    import doctest
    doctest.testmod()



