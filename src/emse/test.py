def function():

    infile = open("values.csv", 'r')
    content = infile.read()       
    infile.close()
    wordList = content.split()

    total = 0
    L = []
    for i in wordList:
        if i.isnumeric():
            total += int(i)
            L.append(i)
    print(min(L))
    print(max(L))
    return total
	
print(function())