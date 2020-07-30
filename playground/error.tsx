import React from 'react';

export const ErrorDisplay = ({ error }: { error: Error }) => {
    return (<pre className="globalError">{error.message}</pre>);
}