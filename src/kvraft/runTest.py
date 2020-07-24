import os

num_tests = 100


if __name__ == "__main__":
    for i in range(num_tests):
        file_name = "out/out" + str(i)
        os.system("go test -v -race -run 3A >" + file_name)
        with open(file_name) as f:
            if 'FAIL:' in f.read():
                print(file_name + " fails")
                continue
            else:
                print(file_name + " ok")

        #os.system("rm " + file_name)

        #print("ROUND " + str(i))
        #os.system("go test -run 2A")
        #print("\n")
