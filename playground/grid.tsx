import React from 'react';
import { ValueDisplay } from './value';

export const DataGrid = ({ data, inline }: { data: any[]; inline?: boolean }) => {
    const columns = Object.keys(data[0]);
    return (<table className="resultsTable">
        <thead>
            <tr>
                {columns.map(k => <th key={k}>{k}</th>)}
            </tr>
        </thead>
        <tbody>
            {
                data.map((x, i) => (<tr key={i}>
                    {
                        columns.map(k => <td key={k}>
                            <ValueDisplay value={x[k]} singleLine={inline} />
                        </td>)
                    }
                </tr>))
            }
        </tbody>
    </table>);
}
