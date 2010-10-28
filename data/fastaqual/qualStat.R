x <- read.table("quals.txt")
m <- mean(x)
s <- sd(x)
write.table(cbind(m,s),"stats.txt")
