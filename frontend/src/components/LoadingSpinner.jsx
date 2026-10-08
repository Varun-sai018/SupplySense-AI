import React from 'react';
import { Loader2 } from 'lucide-react';

export default function LoadingSpinner({ message = 'Loading pipeline telemetry...' }) {
  return (
    <div className="flex flex-col items-center justify-center py-16 px-4 text-center">
      <Loader2 className="w-8 h-8 text-sky-500 animate-spin mb-3" />
      <p className="text-sm text-slate-400 font-medium">{message}</p>
    </div>
  );
}
