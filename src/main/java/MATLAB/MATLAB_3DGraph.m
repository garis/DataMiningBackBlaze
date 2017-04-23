%load forDraw.csx manually using the GUI
MULT=1.5;
POINT_SIZE=1;
AXIS_LIMIT=50;
set(0,'DefaultFigureRenderer','opengl')


C=failure;

VecZero=zeros([size(C,1),1]);

X=Load_UnloadCycleCount;
Y=UncorrectableSectorCount;
Z=ReadChannelMargin;
%ReadChannelMargin
%UncorrectableSectorCount
%Load_UnloadCycleCount
%CurrentPendingSectorCount


color= zeros(size(failure,1),3, 'uint32');
for c = 1:size(C);
    if C(c)==1;
        color(c,1)=1;
        color(c,2)=0;
        color(c,3)=0;
    end
    if C(c)==0;
        color(c,1)=0;
        color(c,2)=1;
        color(c,3)=0;
    end
    if C(c)==-1;
        color(c,1)=0;
        color(c,2)=1;
        color(c,3)=1;
    end
end

newX=X+(1-rand(size(X))*MULT);
newY=Y+(1-rand(size(Y))*MULT);
newZ=Z+(1-rand(size(Z))*MULT);
scatter3(newX,newY,newZ,POINT_SIZE,color)
xlim([-MULT AXIS_LIMIT])
ylim([-MULT AXIS_LIMIT])
zlim([-MULT AXIS_LIMIT])

%set(gca, 'XScale', 'log')
%set(gca, 'YScale', 'log')
%set(gca, 'ZScale', 'log')

Xlabel=xlabel('X'); % x-axis label
set(Xlabel,'FontSize',10);
Ylabel=ylabel('Y'); % y-axis label
set(Ylabel,'FontSize',10);
Zlabel=zlabel('Z'); % y-axis label
set(Zlabel,'FontSize',10);