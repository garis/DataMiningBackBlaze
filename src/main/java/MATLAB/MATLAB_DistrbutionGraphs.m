%ReadChannelMargin
%UncorrectableSectorCount
%Load_UnloadCycleCount
%CurrentPendingSectorCount

X=Load_UnloadCycleCount;
X1=Load_UnloadCycleCount1;

%X=X/size(X,1);
%X1=X1/size(X1,1);

edges = -10:1:1000;

A=discretize(X,edges);
MAXA=max(A);
A=A/MAXA;
B=discretize(X1,edges);
MAXB=max(B);
B=B/MAXB;

figure
subplot(2,1,1)       % add first plot in 2 x 1 grid
plot(A)
title('X=failed')

subplot(2,1,2)       % add second plot in 2 x 1 grid
plot(B)
title('X1=healthy')