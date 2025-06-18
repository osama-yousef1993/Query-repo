import { Document } from 'mongodb';

export default interface Callouts extends Document {
    id?: number;
    title?: string;
    text?: string;
}