function [] = add_nd_var(fnme, varnme, varstandard, varlong, varunits, varprec, varfill, var_scale, var_offset, dimnames, dimlength, chnks);

% for i = 1:length(dimlength)
%     dim_cells{i, 1} = dimnames{i};
%     dim_cells{i, 2} = dimlength(i);
% end

for i = 1:length(dimlength)
    dim_cells{1, i} = dimnames{i};
    dim_cells{2, i} = dimlength(i);
end



if strcmp(varprec, 'NC_DOUBLE')
    prec_out = 'double';
elseif strcmp(varprec, 'NC_FLOAT')
    prec_out = 'single';
elseif strcmp(varprec, 'NC_INT64*')
    prec_out = 'int64';
elseif strcmp(varprec, 'NC_UINT64*')
   prec_out = 'uint64';
elseif strcmp(varprec, 'NC_INT')
   prec_out = 'int32';        
elseif strcmp(varprec, 'NC_UINT*')
   prec_out = 'uint32';            
elseif strcmp(varprec, 'NC_SHORT')
   prec_out = 'int16';   
elseif strcmp(varprec, 'NC_USHORT*')
   prec_out = 'uint16';
elseif strcmp(varprec, 'NC_BYTE')
   prec_out = 'int8'; 
elseif strcmp(varprec, 'NC_UBYTE*')
   prec_out = 'uint8'; 
elseif strcmp(varprec, 'NC_CHAR')
    prec_out = 'char';
end

nccreate(fnme, varnme, 'Dimensions', dim_cells(:), ...
                       'Datatype', prec_out, ...
                       'FillValue', varfill, ...
                       'ChunkSize', chnks, ...
                       'DeflateLevel', 6);
             
if ~isempty(var_scale)
    ncwriteatt(fnme, varnme, 'scale_factor', var_scale);
end
    
if ~isempty(var_offset)
    ncwriteatt(fnme, varnme, 'add_offset', var_offset);
end
    
if ~isempty(varstandard)
    ncwriteatt(fnme, varnme, 'standard_name', varstandard);
end
    
if ~isempty(varlong)
    ncwriteatt(fnme, varnme, 'long_name', varlong);
end
    
if ~isempty(varunits)
    ncwriteatt(fnme, varnme, 'units', varunits);
end