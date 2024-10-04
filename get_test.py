import subprocess

out = 'Hello!'

out_sp = subprocess.check_output(['python', 'test.py', out])
print(out_sp)
