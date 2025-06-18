import { Document } from 'mongodb';
import Callouts from './Callouts';

export default interface AICallouts extends Document {
    callouts?: Callouts[];
}
