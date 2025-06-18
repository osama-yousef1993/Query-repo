import { Document } from 'mongodb';
import AISummary from './AISummary';
import AIHighlights from './AIHighlights';
import AICallouts from './AICallouts';

export default interface AI extends Document {
    highlights?: AIHighlights;
    summary?: AISummary;
    callouts?: AICallouts;
}
