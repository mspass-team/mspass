% This small matlab script does calculations that define the 
% nonothogonal transformation test of test_tcs.   
a=[1.0,1.0,1.0;-1.0,1.0,1.0;0.0,-1.0,0.0]
x1=[1;0;0];
a*x1
xp=[1;1;1];
a*xp
x2=[1;1;0];
a*x2
x3=[0;0;1];
a*x3
arot=[sqrt(2)/2,-sqrt(2)/2,0.0;sqrt(2)/2,sqrt(2)/2,0.0;0.0,0.0,1.0]
at=a*arot
at*x1
at*xp
at*x2
at*x3

