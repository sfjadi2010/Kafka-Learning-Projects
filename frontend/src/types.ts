export interface UploadResult {
    status: string;
    filename: string;
    rows_sent: number;
    topic: string;
    sample_data?: SampleData[];
}

export interface SampleData {
    row_number: number;
    filename: string;
    data: Record<string, string>;
}

export interface KafkaHealth {
    status: string;
    kafka?: string;
    error?: string;
}
