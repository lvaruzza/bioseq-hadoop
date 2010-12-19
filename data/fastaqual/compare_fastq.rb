#!/usr/bin/env ruby

f1 = ARGV[0]
f2 = ARGV[1]

puts "Opening #{f1}  and #{f2}";
hashs = {}

2.times do |i|
  File.open(ARGV[i]) do |f|
    while(!f.eof?) 
      h = f.readline.chomp!
      s = f.readline.chomp!
      f.readline
      q = f.readline.chomp!
      h.gsub!(/_[FR][35]/,"");
      #puts [h,s,q].join("\t")
      hashs[h] = [] if (hashs[h] == nil)
      hashs[h][i]=[s,q]
    end
  end
end

$valid = true

for (k,v) in hashs
  puts k
  puts "\t" + v[0][0]
  puts "\t" + v[1][0]
  if (v[0][0] != v[1][0])
    $valid = false 
    puts "INVALID SEQ!!!!!"
  end

  puts "\t " + v[0][1]
  puts "\t" + v[1][1]
  v[1][1].gsub!(/^./,"");
  puts v[1][1]
  puts v[0][1]
  if (v[1][1] != v[1][0])
    $valid = false 
    puts "INVALID QUAL!!!!!!!"
  end
end

if $valid
  puts "The two files are equivalent"
else
  puts "ERROR!!! THE FILES ARE NOT EQUIVALENT"
end
