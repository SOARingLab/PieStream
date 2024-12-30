package org.piestream.engine;

/**
 * Configuration data structure containing runtime settings.
 * Implemented as a singleton to allow global access without passing parameters.
 */
public class RuntimeSet {
    private boolean loggerResultASAPConsole;
    private boolean recordAVGProceTime;

    // 私有静态实例，确保唯一性
    private static volatile RuntimeSet instance = null;

    // 私有构造方法，防止外部实例化
    private RuntimeSet(boolean loggerResultASAPConsole, boolean recordAVGProceTime) {
        this.loggerResultASAPConsole = loggerResultASAPConsole;
        this.recordAVGProceTime = recordAVGProceTime;
    }

    /**
     * 获取单例实例的方法，使用双重检查锁定确保线程安全。
     *
     * @return RuntimeSet 单例实例
     */
    public static RuntimeSet getInstance() {
        if (instance == null) {
            synchronized (RuntimeSet.class) {
                if (instance == null) {
                    // 设置默认配置，可以根据需要修改
                    instance = new RuntimeSet(false, false);
                }
            }
        }
        return instance;
    }

    /**
     * 初始化单例实例的方法，仅在应用程序启动时调用一次。
     *
     * @param loggerResultASAPConsole 是否启用立即日志输出
     * @param recordAVGProceTime       是否记录平均处理时间
     */
    public static void initialize(boolean loggerResultASAPConsole, boolean recordAVGProceTime) {
        if (instance == null) {
            synchronized (RuntimeSet.class) {
                if (instance == null) {
                    instance = new RuntimeSet(loggerResultASAPConsole, recordAVGProceTime);
                }
            }
        } else {
            throw new IllegalStateException("RuntimeSet has already been initialized.");
        }
    }

    // Getter 和 Setter 方法
    public boolean isLoggerResultASAPConsole() {
        return loggerResultASAPConsole;
    }

    public boolean isRecordAVGProceTime() {
        return recordAVGProceTime;
    }

}
