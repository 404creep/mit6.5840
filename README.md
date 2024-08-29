# 6.5840 Lab
官方网站：https://pdos.csail.mit.edu/6.824/labs/lab-raft.html
## Study Notes

mapreduce 结构 https://www.mubu.com/doc/2qvlcfblV6b

raft 实现总结 https://www.mubu.com/doc/2prW5zqFYmb

raft 一致性总结 https://www.mubu.com/doc/1ZoHdMBk9Cb

raft test和踩坑记录 https://mubu.com/app/edit/home/2KNq7ixmm_r

raft 论文 https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf

参考资料 https://mit-public-courses-cn-translatio.gitbook.io/mit6-824/lecture-07-raft2/7.3-hui-fu-jia-su-backup-acceleration

## Lab2 Raft
1. 进入测试目录
```bash
cd .\6.5840\src\raft
```
2. 测试某一个部分
```bash
go test -run 2A
go test -run TestInitialElection2A
```
3. 测试全部
```bash
go test
```

4. 脚本批量测试
```bash
python3 dstest.py -n 100 -p 128 2A 2B 2C 2D # (linux)
python dstest.py -n 100 -p 128 2A 2B 2C 2D # (windows)
```

5.批量测试case
```bash
python dstest.py -n 100 -p 128 -e 2C
python dstest.py -n 100 -p 128 TestRPCBytes2B 
```





### raft部分框架图
![img.png](img.png)