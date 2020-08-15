
class Q1 {
    public static void calculate(int l,int k,float array[][]) {
        if(l != 1) {
            array[l][k] = (6-l)*array[l-1][k-1]/(500-k+1)+array[l][k-1]-(6-l-1)*array[l][k-1]/(500-k+1);
        }
        else {
            array[l][k] = k*40000-2*array[2][k]-3*array[3][k]-4*array[4][k]-5*array[5][k];
        }
    }
    
    
    public static void main(String[] args) {
        int N = 500;
        int R = 20000000;   
        float[][] array = new float[6][201];
        for(int i = 0;i < 6;i++) {
            for(int j = 0;j < 201;j++) {
                array[i][j] = 0;
            }
        }
        array[1][1] = R/N;
        for(int k = 2; k < 201;k++) {
            for(int l = 5; l > 0;l--) {
                calculate(l,k,array);
            }
        }
        System.out.println(array[5][200]);

    }
}